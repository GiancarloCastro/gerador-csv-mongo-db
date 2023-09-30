package com.mongodb.via;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.config.MongoConnection;

public class CaracteristicasCelular {

    private static final char SEPARATOR_COLUMN = ';';

    private static final char SEPARATOR_SKUS = ',';

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static boolean isPrimeiraLinha = true;

    public static void main(String[] args)  {
        try {
            FindIterable<Document> result = getConexaoMongoDb(args);

           // geraCsvParaCelulares(result, "celulares-processador.csv" , "Processador");
           // geraCsvParaCelulares(result, "celulares-armazenamento.csv", "Armazenamento");
           // geraCsvParaCelulares(result, "celulares-memoria.csv", "Memoria");
           // geraCsvParaCelulares(result, "celulares-tela.csv", "Tela");
            geraCsvParaCelulares(result, "celulares-camera.csv", "Camera");

            System.out.println("Finalizou");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static FindIterable<Document> getConexaoMongoDb(String[] args) {
        MongoClient mongoStress = MongoConnection.createConnection(args);
        MongoDatabase catalogo = mongoStress.getDatabase("catalogo");
        Document filter = Document.parse("{\"slugCategoria\":\"Smartphones-Categoria\"}");
        Document projection = Document.parse("{\"nome\":1, \"caracteristicas\":1, \"situacao\": 1}");

        return catalogo.getCollection("produtos").find(filter).projection(projection);
    }

    private static void geraCsvParaCelulares(FindIterable<Document> result, String nomeCsv, String TipoDoDado) throws IOException {
        File csv = new File(nomeCsv);
        System.out.println("Arquivo: " + csv.getAbsolutePath());
        FileOutputStream out = new FileOutputStream(csv);

        MongoCursor<Document> cursor = result.cursor();
        while (cursor.hasNext()) {
            Document registro = cursor.next();
            Document registroId = (Document) registro.get("_id");
            List<Document> caracteristicas = registro.getList("caracteristicas", Document.class);
            String nome = registro.getString("nome");
            String situacao = buscaSituacao(registro.getString("situacao"));
            printline(out,
                    registroId.getLong("sku"),
                    registroId.getInteger("tipoProduto"),
                    situacao,
                    processador(nome, TipoDoDado),
                    processador(caracteristicas, TipoDoDado),
                    nome
            );
        }
        cursor.close();
    }

    private static String buscaSituacao(String situacao) {
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

    private static String processador(List<Document> caracteristicas, String tipoDoDado) {
        switch (tipoDoDado) {
            case "Processador":
                return localizaNomeDaCaracteristica("Processador", caracteristicas);
            case "Memoria":
                return localizaNomeDaCaracteristica("RAM", caracteristicas);
            case "Armazenamento":
                return localizaNomeDaCaracteristica("Memoria-interna-", caracteristicas);
            case "Tela":
                return localizaNomeDaCaracteristica("Caracteristicas-Gerais", caracteristicas);
            case "Camera":
                return localizaNomeDaCaracteristica("Resolucao-camera-traseira", caracteristicas);
            default:
                return "";
        }
    }

    private static String localizaNomeDaCaracteristica(String nomeDaCaracteristica, List<Document> caracteristicas) {
        if (caracteristicas != null)
            for (Document caracteristica : caracteristicas) {
                if (nomeDaCaracteristica.equalsIgnoreCase(caracteristica.getString("slug")) || verificaSeCameraTraseira(nomeDaCaracteristica, caracteristica)) {
                    List<String> valores = caracteristica.getList("valores", String.class);
                    StringBuilder sb = new StringBuilder(128);
                    for (String valor : valores) {
                        if (nomeDaCaracteristica.equalsIgnoreCase("Caracteristicas-Gerais") && !valor.contains("Tela"))
                            continue;

                        sb.append("\"").append("[").append(valor).append("]").append("\"").append(",");
                    }
                    if (sb.length() > 0)
                        if (nomeDaCaracteristica.equals("Resolucao-camera-traseira"))
                            return ajustaTrechoDescricao("Câmera", sb.toString());
                        return sb.substring(0, sb.length() - 1);
                }
            }
        return "";
    }

    private static boolean verificaSeCameraTraseira(String nomeDaCaracteristica, Document caracteristica) {
        if (!nomeDaCaracteristica.equals("Resolucao-camera-traseira"))
          return false;

        String slug = caracteristica.getString("slug");

        return slug.toLowerCase().contains("camera") && slug.toLowerCase().contains("traseira");
    }

    private static String processador(String nome, String tipoDoDado) {
        switch (tipoDoDado)  {
            case "Processador":
                return retornaSubstringDaCaracteristica(nome, "Processador");
            case "Armazenamento":
                return retornaSubstringDaCaracteristica(nome, "Armazenamento");
            case "Memoria":
                return retornaSubstringDaCaracteristica(nome, "Memoria");
            case "Tela":
                return retornaSubstringDaCaracteristica(nome, "Tela");
            case "Camera":
                return retornaSubstringDaCaracteristica(nome, "Câmera");
            default:
                return "";
        }
    }

    private static String retornaSubstringDaCaracteristica (String nome, String tipoDoDado) {
        String caracteristica;

        if (tipoDoDado.equals("Armazenamento"))
            caracteristica = "GB";
        else if (tipoDoDado.equals("Memoria"))
            caracteristica = "RAM";
        else if (tipoDoDado.equals("Processador"))
            caracteristica = tipoDoDado;
        else if (tipoDoDado.equals("Câmera"))
            caracteristica = "MP";
        else if (tipoDoDado.equals("Tela"))
            caracteristica = "\"";
        else
            return "";

        char caractere;
        int start = nome.indexOf(caracteristica);
        if (tipoDoDado.equals("Tela") && start == -1) {
            caracteristica = "”";
            start = nome.indexOf(caracteristica);
            if (start == -1) {
                caracteristica = "'";
                start = nome.indexOf(caracteristica);
            }

            if (start == -1) {
                caracteristica = "’";
                start = nome.indexOf(caracteristica);
            }
        } else if (tipoDoDado.equals("Armazenamento") && start == -1){
            caracteristica = "TB";
            start = nome.indexOf(caracteristica);
            if (start == -1) {
                caracteristica = "MB";
                start = nome.indexOf(caracteristica);
            }
        }

        if (start > -1) {
            if (caracteristica.equals("GB")) {
                int inicio;
                for (int i = start - 1; i >= 0; i--) {
                    caractere = nome.charAt(i);
                    inicio = i + 1;
                    if (i == start - 1) {
                        inicio++;
                    }
                    if (!Character.isDigit(caractere) ) {
                        return nome.substring(inicio, start + 2);
                    }
                }

                return nome.substring(start);
            } else if (caracteristica.equals("RAM")) {
                for (int i = start; i>=0; i--) {
                    caractere = nome.charAt(i);
                    if (Character.isDigit(caractere)) {
                        return nome.substring(i, i + 3);
                    }
                }
            } else if (caracteristica.equals("\"") ||
                    caracteristica.equals("”") ||
                    caracteristica.equals("'") ||
                    caracteristica.equals("’")) {
                for (int i = start-1; i>=0; i--) {
                    caractere = nome.charAt(i);
                    if (!Character.isDigit(caractere) && caractere != '.' && caractere != ',') {
                        if (caracteristica.equals("'") || caracteristica.equals("’"))
                            return nome.substring(i+1, start+2);
                        else
                            return nome.substring(i+1, start+1);
                    }
                }
            } else {
                if (tipoDoDado.equals("Câmera"))
                    start = -1;

               return ajustaTrechoDescricao(tipoDoDado, nome.substring(start+tipoDoDado.length()+1));
            }
        }
        return "";
    }

    private static String ajustaTrechoDescricao(String tipoDoDado, String descricao) {
        StringBuilder sb = new StringBuilder(128);

        if (tipoDoDado.equals("Câmera")) {
            int start = descricao.indexOf("MP");

            for (int i = start; i >= 0; i--) {
                char digito = descricao.charAt(i);
                if (Character.isDigit(digito) ){
                    sb.insert(0, digito);
                } else if (Character.isSpaceChar(digito)) {
                    if (start != i)
                        break;
                } else if (digito == '.' || digito == ',') {
                    sb.insert(0, digito);
                }
            }

            if (sb.length() > 0 ) {
                descricao = sb.append("MP").toString();
                if (!Character.isDigit(descricao.charAt(0)))
                    descricao = descricao.substring(1, sb.length());
            } else {
                descricao = "";
            }
        }

        return descricao;
    }

    private static void printline(OutputStream out, Long conjuntoSku, Integer tipoProduto, String... values)
            throws IOException {

        if (isPrimeiraLinha) {
            String headerLine = "Id;TipoProduto;Situacao;Desc. Nome; Desc. Carac.;Nome;" + LINE_SEPARATOR;
            out.write(headerLine.getBytes(Charset.forName("CP1252")));
            isPrimeiraLinha = false;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(conjuntoSku).append(SEPARATOR_COLUMN).append(tipoProduto).append(SEPARATOR_COLUMN);
        for (String value : values) {
            sb.append(value).append(SEPARATOR_COLUMN);
        }
        sb.append(LINE_SEPARATOR);

        String line = sb.toString();
        out.write(line.getBytes(Charset.forName("CP1252")));
    }
}
