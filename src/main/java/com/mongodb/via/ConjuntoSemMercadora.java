package com.mongodb.via;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.config.MongoConnection;

public class ConjuntoSemMercadora {

    private static final char SEPARATOR_COLUMN = ';';

    private static final char SEPARATOR_SKUS = ',';

    private static final String LINE_SEPARATOR = System.lineSeparator();

    public static void main(String[] args) throws IOException {
        File csv = new File("produtos.csv");
        System.out.println("Arquivo: " + csv.getAbsolutePath());
        FileOutputStream out = new FileOutputStream(csv);
        try (MongoClient mongoStress = MongoConnection.createConnection(args)) {
            MongoDatabase catalogo = mongoStress.getDatabase("catalogo");
            Document filter = Document.parse("{\"_id.tipoProduto\":1}");
            Document projection = Document.parse("{\"nome\":1}");
            FindIterable<Document> result = catalogo.getCollection("produtos").find(filter).projection(projection);
            MongoCursor<Document> cursor = result.cursor();
            Set<Long> mercadorias = new HashSet<>();
            while (cursor.hasNext()) {
                Document document = cursor.next();
                Document mercadoriaId = document.get("_id", Document.class);
                mercadorias.add(mercadoriaId.getLong("sku"));
            }
            cursor.close();
            filter = Document.parse("{\"_id.tipoProduto\":2}");
            projection = Document.parse("{\"nome\":1,\"itensConjunto\":1}");
            result = catalogo.getCollection("produtos").find(filter).projection(projection);
            cursor = result.cursor();
            while (cursor.hasNext()) {
                Document conjunto = cursor.next();
                Document conjuntoId = (Document) conjunto.get("_id");
                List<Document> conjuntoItens = conjunto.getList("itensConjunto", Document.class);
                List<Document> conjuntoItensTemp = new ArrayList<>(conjuntoItens);
                Iterator<Document> iteratorItens = conjuntoItensTemp.iterator();
                while (iteratorItens.hasNext()) {
                    Document item = iteratorItens.next();
                    Long itemSku = item.getLong("sku");
                    if (mercadorias.contains(itemSku)) {
                        iteratorItens.remove();
                    }
                }
                if (conjuntoItensTemp.size() > 0) {
                    printline(out, conjuntoId.getLong("sku"), conjunto.getString("nome"), conjuntoItens,
                            conjuntoItensTemp);
                }
            }
        }
    }

    private static void printline(OutputStream out, Long conjuntoSku, String conjuntoNome, List<Document> itens,
            List<Document> itensFaltantes) throws IOException {
        StringBuilder sb = new StringBuilder(256);
        sb.append(conjuntoSku).append(SEPARATOR_COLUMN);
        printItens(itens, sb);
        printItens(itensFaltantes, sb);
        sb.append(itens.size()).append(SEPARATOR_COLUMN).append(itensFaltantes.size()).append(SEPARATOR_COLUMN)
                .append(conjuntoNome).append(LINE_SEPARATOR);
        out.write(sb.toString().getBytes());
    }

    private static void printItens(List<Document> itens, StringBuilder sb) {
        for (Document item : itens) {
            sb.append(item.getLong("sku")).append(SEPARATOR_SKUS);
        }
        sb.delete(sb.length() - 1, sb.length());
        sb.append(SEPARATOR_COLUMN);
    }

}
