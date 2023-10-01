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

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static boolean isPrimeiraLinha;

    public static void main(String[] args)  {
        try {
            MongoClient mongoConnection = MongoConnection.createConnection(args);
            FindIterable<Document> result = getDadosCelularesMongo(mongoConnection);

            geraCsvParaCelulares(result, "celulares-processador.csv" , "Processador");
            geraCsvParaCelulares(result, "celulares-armazenamento.csv", "Armazenamento");
            geraCsvParaCelulares(result, "celulares-memoria.csv", "Memoria");
            geraCsvParaCelulares(result, "celulares-tela.csv", "Tela");
            geraCsvParaCelulares(result, "celulares-camera.csv", "Camera");

            mongoConnection.close();

            System.out.println("Finalizou");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static FindIterable<Document> getDadosCelularesMongo(MongoClient mongoConnection) {
        MongoDatabase catalogo = mongoConnection.getDatabase("catalogo");
        Document filter = Document.parse("{\"slugCategoria\":\"Smartphones-Categoria\"}");
        Document projection = Document.parse("{\"nome\":1, \"caracteristicas\":1, \"situacao\": 1}");

        return catalogo.getCollection("produtos").find(filter).projection(projection);
    }

    private static void geraCsvParaCelulares(FindIterable<Document> result, String nomeCsv, String tipoDoDado) throws IOException {
        File csv = new File(nomeCsv);
        System.out.println("Arquivo: " + csv.getAbsolutePath());
        FileOutputStream out = new FileOutputStream(csv);

        isPrimeiraLinha = true;

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
                    retornaNomeDaCaracteristica(nome, tipoDoDado),
                    retornaNomeDaCaracteristica(caracteristicas, tipoDoDado),
                    nome
            );
        }
        cursor.close();
        out.close();
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

    private static String retornaNomeDaCaracteristica(List<Document> caracteristicas, String tipoDoDado) {
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
                    if (sb.length() > 0) {
                        if (nomeDaCaracteristica.equals("Resolucao-camera-traseira") || nomeDaCaracteristica.equals("Memoria-interna-") || nomeDaCaracteristica.equals("Caracteristicas-Gerais")  ) {
                            return ajustaTrechoDescricao(nomeDaCaracteristica, sb.toString());
                        }
                        return sb.substring(0, sb.length() - 1);
                    }
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

    private static String retornaNomeDaCaracteristica(String nome, String tipoDoDado) {
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
            if (caracteristica.equals("GB") || caracteristica.equals("TB") || caracteristica.equals("MB")) {
                return extraiTamanhoArmazenamento(start, nome);
            } else if (caracteristica.equals("RAM")) {
                int indiceRAM = retornaIndiceRam(nome);
                String ram = null;
                if (indiceRAM > 0){
                    for (int i = start; i>=0; i--) {
                        caractere = nome.charAt(i);
                        if (Character.isDigit(caractere)) {
                            ram = nome.substring(i, i + 3);
                            break;
                        }
                    }
                } else {
                    for (int i = start; i < nome.length(); i++) {
                        caractere = nome.charAt(i);
                        if (Character.isDigit(caractere)) {
                            ram = nome.substring(i, i + 3);
                            break;
                        }
                    }
                }
                assert ram != null;
                return ram.replace(',', 'B');

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
            } else if (caracteristica.equals("Processador")) {
                int end = nome.toLowerCase().indexOf("ghz", start);

                if (end != -1) {
                    end = end + 3;
                } else {
                    end = nome.toLowerCase().indexOf("core", start);

                    if (end != -1)
                        end = end + 4;
                    else
                        end = nome.length();
                }

                return nome.substring(start+tipoDoDado.length()+1, end).replace("de ", "");

            } else {
                start = -1;

               return ajustaTrechoDescricao(tipoDoDado, nome.substring(start+tipoDoDado.length()+1));
            }
        }
        return "";
    }

    private static int retornaIndiceRam(String nome) {
        int indiceRAM = nome.indexOf("RAM");
        char caractere;

        for (int i = indiceRAM; i >= 0; i--) {
            caractere = nome.charAt(i);

            if ((caractere == 'G') || (caractere == 'M' && nome.charAt(i+1) == 'B')){
                return indiceRAM - i > 7 ? 0 : 1;
            }
        }

        return 1;
    }

    private static String ajustaTrechoDescricao(String tipoDoDado, String descricao) {
        StringBuilder sb = new StringBuilder(128);
        String tipo;

        switch (tipoDoDado) {
            case "Resolucao-camera-traseira":
                tipo = "Câmera";
                break;
            case "Memoria-interna-":
                tipo = "Armazenamento";
                break;
            case "Caracteristicas-Gerais":
                tipo = "Tela";
                break;
            default:
                tipo = tipoDoDado;
                break;
        }

        if (tipo.equals("Câmera")) {
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
        } else if (tipo.equals("Armazenamento")) {
            int start = descricao.indexOf("GB");
            if (start == -1) {
                start = descricao.indexOf("TB");
                if (start == -1)
                    start = descricao.indexOf("MB");
            }

            descricao = extraiTamanhoArmazenamento(start, descricao);
        } else if (tipo.equals("Tela")){
            descricao = removeTag(descricao);
            descricao = descricao.replace("-", "");
        }

        return descricao;
    }

    private static String removeTag(String descricao) {
        int indiceTagInicial = descricao.indexOf("<");
        int indiceTagFinal = descricao.indexOf(">");

        if (indiceTagInicial != -1){
            String parteAntes = descricao.substring(0, indiceTagInicial);

            String parteDepois = descricao.substring(indiceTagFinal+1);

            return removeTag(parteAntes + parteDepois);
        }

        return descricao;
    }

    private static String extraiTamanhoArmazenamento(int start, String nome) {
        if (start == -1){
            return "";
        }

        int inicio;
        int espacos = 0;
        char caractere;
        for (int i = start - 1; i >= 0; i--) {
            caractere = nome.charAt(i);

            if (Character.isSpaceChar(caractere) && espacos == 0 && i == start - 1){
                espacos++;
                continue;
            }

            inicio = i + 1;
            if (i == start - 1) {
                inicio++;
            }
            if (!Character.isDigit(caractere) ) {
                return nome.substring(inicio, start + 2);
            }
        }

        return nome.substring(start);
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
