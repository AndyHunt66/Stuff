package transporttest;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

public class BasicTransportWith {
    public static void main(final String[] args) {


        TransportClient client = new PreBuiltXPackTransportClient(
                Settings.builder().put("cluster.name", "elasticsearch")
                        .build());


        try {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        IndicesAdminClient indicesAdminClient = client.admin().indices();

        GetSettingsResponse response = client.admin().indices().prepareGetSettings().get();
        for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);

            System.out.println( "Index:\t" + index + "\n\tshards:\t"+shards+"\n\treplicas:\t"+replicas);
        }

        client.close();
    }
}
