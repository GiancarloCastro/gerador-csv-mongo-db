package com.mongodb.via;

import org.json.JSONObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.config.MongoConnection;
import com.mongodb.model.ProdutoComImagemInvalidaDto;
import com.mongodb.utils.TrustAllX509TrustManager;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.bson.Document;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
@AllArgsConstructor
public class ProdutosSemImagemCadastrada {

    private static final char SEPARATOR_COLUMN = ';';

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static boolean isPrimeiraLinha;


    public static void main(String[] args) throws IOException {
        File csv = new File("produtos-sem-imagem-cadastrada.csv");
        System.out.println("Arquivo: " + csv.getAbsolutePath());

        try (FileOutputStream out = new FileOutputStream(csv);
            MongoClient mongoConnection = MongoConnection.createConnection(args)) {
            int batchSize = 1000;
            int skip = 0;
            boolean hasMore;
            AtomicInteger cont = new AtomicInteger();

            AtomicInteger quantidadeCD = new AtomicInteger(0);

            int numThreads = 5;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            do {
                FindIterable<Document> result = getDadosProdutos(mongoConnection, skip, batchSize);
                long quantidadeDeRegistros = getQuantidadeDeRegistros(mongoConnection);

                MongoCursor<Document> cursor = result.iterator();
                AtomicInteger estoque = new AtomicInteger();
                AtomicInteger estoqueCD = new AtomicInteger();

                Object lock1 = new Object();
                Object lock2 = new Object();
                Object lock3 = new Object();
                hasMore = false;
                isPrimeiraLinha = true;

                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    executor.submit(() -> {
                        Document produtoID = document.get("_id", Document.class);

                        FindIterable<Document> resultEstoque = getDadosEstoque(mongoConnection, produtoID);
                        final MongoCursor<Document> cursorEstoque = resultEstoque.cursor();

                        estoque.set(0);

                        Map<Integer, Integer> depositosAlternativos = new HashMap<>();

                        while (cursorEstoque.hasNext()) {
                            Document documentEstoque = cursorEstoque.next();
                            Document estadoProduto = documentEstoque.get("estadoProduto", Document.class);

                            Document estoquePadrao = null;
                            Document estoqueCDDocument = null;
                            if (estadoProduto != null)
                                estoquePadrao = estadoProduto.get("PADRAO", Document.class);

                            if (estoquePadrao != null) {
                                synchronized (lock1) {
                                    estoque.set(estoque.get() + estoquePadrao.get("quantidade", Integer.class));
                                }

                                estoqueCDDocument = estadoProduto.get("CD", Document.class);
                            }

                            if (estoqueCDDocument != null) {
                                Document documentDeposito = estoqueCDDocument.get("depositoAlternativo", Document.class);

                                if (documentDeposito != null) {
                                    Set<String> chaves = documentDeposito.keySet();
                                    for (String chave : chaves) {
                                        depositosAlternativos.put(Integer.parseInt(chave), documentDeposito.get(chave, Integer.class));
                                    }

                                    AtomicInteger quantidadeDepositosAlternativos = new AtomicInteger(0);
                                    depositosAlternativos.forEach((dep, qtd) -> quantidadeDepositosAlternativos.set(quantidadeDepositosAlternativos.get() + qtd));

                                    quantidadeCD.set(quantidadeDepositosAlternativos.get());
                                } else {
                                    quantidadeCD.set(0);
                                }
                            } else {
                                quantidadeCD.set(0);
                            }
                            synchronized (lock2) {
                                estoqueCD.set(Math.max(quantidadeCD.get(), 0));
                            }
                        }

                        ProdutoComImagemInvalidaDto produtoComImagemInvalidaDto = ProdutoComImagemInvalidaDto.builder()
                                .sku(produtoID.get("sku", Long.class))
                                .skuExterno(document.get("skuExterno", Long.class))
                                .nome(document.get("nome", String.class))
                                .departamento(document.get("slugDepartamento", String.class))
                                .quantidadePadrao(estoque.get())
                                .quantidadeCD(estoqueCD.get())
                                .emLinhaForaDeLinha(retornaSituacao(document.get("situacao", String.class)))
                                .motivo(buscaMotivo(document.get("skuExterno", Long.class)))
                                .build();

                        try {
                            if (!produtoComImagemInvalidaDto.getMotivo().isEmpty())
                                printline(out, produtoComImagemInvalidaDto);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }

                        synchronized (lock3) {
                            System.out.print("\rProdutos verificados: " + cont.incrementAndGet() + " de " + quantidadeDeRegistros);
                        }

                    });
                    hasMore = true;
                }

                skip += batchSize;

                cursor.close();
            } while (hasMore);
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            System.out.println("Arquivo Gerado com sucesso!");
        } catch (Exception e) {
            System.out.println("Erro na geração do arquivo:"+e.getCause());
            e.printStackTrace();
        }
    }

    private static long getQuantidadeDeRegistros(MongoClient mongoConnection) {
        MongoDatabase catalogo = mongoConnection.getDatabase("catalogo");

        return catalogo.getCollection("produtos").countDocuments();
    }

    private static String buscaMotivo(Long skuExterno) {
        try {
            if (skuExterno == null)
                return "";

            doTrustToCertificates();

            String url = "https://api-imagens-leitura.viavarejo.com.br/Imagem/" + skuExterno;
            URL obj = new URL(url);

            HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();

            BufferedReader in;
            if (responseCode == HttpURLConnection.HTTP_OK) {
                in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            } else {
                in = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
            }

            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            if (responseCode != HttpURLConnection.HTTP_OK) {
                String errorResponse = response.toString();
                try {
                    JSONObject jsonResponse = new JSONObject(errorResponse);
                    return jsonResponse.getString("errorMessage");
                } catch (Exception e) {
                    return errorResponse;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
    public static void doTrustToCertificates() throws Exception {
        URL url = new URL("https://api-imagens-leitura.viavarejo.com.br");
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] { new TrustAllX509TrustManager() }, new SecureRandom());
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        connection.setSSLSocketFactory(sslSocketFactory);

        HostnameVerifier hostnameVerifier = (hostname, session) -> true;
        connection.setHostnameVerifier(hostnameVerifier);

        connection.connect();
    }


    private static String retornaSituacao(String situacao) {
        switch (situacao){
            case "A":
            case "F":
                return "EM LINHA";
            case "S":
                return "FORA DE LINHA";
            default:
                return "";
        }
    }

    private static FindIterable<Document> getDadosEstoque(MongoClient mongoConnection, Document produtoID) {
        MongoDatabase catalogo = mongoConnection.getDatabase("catalogo");

        Document filter = Document.parse("{\"_id.sku\":" + produtoID.get("sku", Long.class) + ", " + "\"_id.tipoProduto\":" + produtoID.get("tipoProduto", Integer.class) + "}");

        return catalogo.getCollection("estoque").find(filter);
    }

    private static FindIterable<Document> getDadosProdutos(MongoClient mongoConnection, int skip, int batchSize) {
        MongoDatabase catalogo = mongoConnection.getDatabase("catalogo");
        Document projection = Document.parse("{\"skuExterno\": 1, \"nome\": 1, \"slugDepartamento\": 1, \"situacao\": 1}");

        return catalogo.getCollection("produtos").find().skip(skip).limit(batchSize).projection(projection);
    }
    private static void printline(OutputStream out, ProdutoComImagemInvalidaDto produtoComImagemInvalidaDto)
            throws IOException {

        if (isPrimeiraLinha) {
            String headerLine = "Sku;SkuExterno;Nome;Departamento;Quantidade Padrão;Quantidade CD;Situação;Motivo" + LINE_SEPARATOR;
            out.write(headerLine.getBytes(Charset.forName("CP1252")));
            isPrimeiraLinha = false;
        }

        String line = String.valueOf(produtoComImagemInvalidaDto.getSku()) + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getSkuExterno() + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getNome() + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getDepartamento() + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getQuantidadePadrao() + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getQuantidadeCD() + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getEmLinhaForaDeLinha() + SEPARATOR_COLUMN +
                produtoComImagemInvalidaDto.getMotivo() + SEPARATOR_COLUMN +
                LINE_SEPARATOR;

        out.write(line.getBytes(Charset.forName("CP1252")));
    }
}
