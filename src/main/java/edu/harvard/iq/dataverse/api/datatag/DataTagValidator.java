package edu.harvard.iq.dataverse.api.datatag;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.json.JsonObject;
import java.util.logging.Logger;

//This has been made a singleton because the schema validator only needs to exist once
//and loads very slowly
public class DataTagValidator {
    public enum Type {CONFIGURATION, API_BODY};

    private static final Logger logger = Logger.getLogger(DataTagValidator.class.getCanonicalName());
    private static DataTagValidator dtvSingleton;

    private DataTagValidator() {}
    
    static {
        dtvSingleton = new DataTagValidator();
    }
    
    public static DataTagValidator getInstance() {
        return dtvSingleton;
    }


    public boolean isDataTagValid(String jsonInput, Type type) {
        try {
            switch (type) {
                case CONFIGURATION: dtscScheme.validate(new JSONObject(jsonInput));
                    break;
                case API_BODY: schema.validate(new JSONObject(jsonInput)); // throws a ValidationException if this object is invalid
                    break;
            }

        } catch (ValidationException vx) {
            logger.info("DataTag (" + type.name() + " schema error : " + vx); //without classLoader is blows up in actual deployment
            return false;
        } catch (Exception ex) {
            logger.info("DataTag (" + type.name() +" file error : " + ex);
            return false;
        }

        return true;
    }
    public String getPrettyJsonString(JsonObject jsonObject) {
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(jsonObject.toString());
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(je);
    }

    private static final String dtSchema ="{\n" +
            "  \"definitions\": {},\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"$id\": \"http://example.com/root.json\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"title\": \"The Root Schema\",\n" +
            "  \"required\": [\n" +
            "    \"schemaName\",\n" +
            "    \"schemaVersion\",\n" +
            "    \"schemaPid\",\n" +
            "    \"dataTagsServiceUrl\",\n" +
            "    \"tag\",\n" +
            "    \"colorCode\",\n" +
            "    \"outcomeTime\",\n" +
            "    \"questionAnswerList\"\n" +
            "  ],\n" +
            "  \"properties\": {\n" +
            "    \"schemaName\": {\n" +
            "      \"$id\": \"#/properties/schemaName\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Schemaname Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"dans\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"schemaVersion\": {\n" +
            "      \"$id\": \"#/properties/schemaVersion\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Schemaversion Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"v1\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"schemaPid\": {\n" +
            "      \"$id\": \"#/properties/schemaPid\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Schemapid Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"hdl:10411/abcedfg123\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"dataTagsServiceUrl\": {\n" +
            "      \"$id\": \"#/properties/dataTagsServiceUrl\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Datatagsserviceurl Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"http://akmi:8888/dans/v1\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"tag\": {\n" +
            "      \"$id\": \"#/properties/tag\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Tag Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"RED\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"colorCode\": {\n" +
            "      \"$id\": \"#/properties/colorCode\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Colorcode Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"#FFA500\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"outcomeTime\": {\n" +
            "      \"$id\": \"#/properties/outcomeTime\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Outcometime Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"2020-02-12 13:17:31\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"questionAnswerList\": {\n" +
            "      \"$id\": \"#/properties/questionAnswerList\",\n" +
            "      \"type\": \"array\",\n" +
            "      \"title\": \"The Questionanswerlist Schema\",\n" +
            "      \"items\": {\n" +
            "        \"$id\": \"#/properties/questionAnswerList/items\",\n" +
            "        \"type\": \"object\",\n" +
            "        \"title\": \"The Items Schema\",\n" +
            "        \"required\": [\n" +
            "          \"question\",\n" +
            "          \"answer\"\n" +
            "        ],\n" +
            "        \"properties\": {\n" +
            "          \"question\": {\n" +
            "            \"$id\": \"#/properties/questionAnswerList/items/properties/question\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"title\": \"The Question Schema\",\n" +
            "            \"default\": \"\",\n" +
            "            \"examples\": [\n" +
            "              \"\"\n" +
            "            ],\n" +
            "            \"pattern\": \"^(.*)$\"\n" +
            "          },\n" +
            "          \"answer\": {\n" +
            "            \"$id\": \"#/properties/questionAnswerList/items/properties/answer\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"title\": \"The Answer Schema\",\n" +
            "            \"default\": \"\",\n" +
            "            \"examples\": [\n" +
            "              \"Take me to the first question\"\n" +
            "            ],\n" +
            "            \"pattern\": \"^(.*)$\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    private static final JSONObject rawSchema = new JSONObject(new JSONTokener(dtSchema));
    private static final Schema schema = SchemaLoader.load(rawSchema);

    private static final String dtsConfScheme = "{\n" +
            "  \"definitions\": {},\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"$id\": \"http://example.com/root.json\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"title\": \"The Root Schema\",\n" +
            "  \"required\": [\n" +
            "    \"dataTagServiceUrl\",\n" +
            "    \"encryptKey\",\n" +
            "    \"validity-duration\"\n" +
            "  ],\n" +
            "  \"properties\": {\n" +
            "    \"dataTagServiceUrl\": {\n" +
            "      \"$id\": \"#/properties/dataTagServiceUrl\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Datatagserviceurl Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"https://amalin.nl:8888/dans/v1\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"encryptKey\": {\n" +
            "      \"$id\": \"#/properties/encryptKey\",\n" +
            "      \"type\": \"string\",\n" +
            "      \"title\": \"The Encryptkey Schema\",\n" +
            "      \"default\": \"\",\n" +
            "      \"examples\": [\n" +
            "        \"Amalin2004\"\n" +
            "      ],\n" +
            "      \"pattern\": \"^(.*)$\"\n" +
            "    },\n" +
            "    \"validity-duration\": {\n" +
            "      \"$id\": \"#/properties/validity-duration\",\n" +
            "      \"type\": \"integer\",\n" +
            "      \"title\": \"The Validity-duration Schema\",\n" +
            "      \"default\": 0,\n" +
            "      \"examples\": [\n" +
            "        600\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";

    private static final JSONObject rawdtsConfScheme = new JSONObject(new JSONTokener(dtsConfScheme));
    private static final Schema dtscScheme = SchemaLoader.load(rawdtsConfScheme);
}
