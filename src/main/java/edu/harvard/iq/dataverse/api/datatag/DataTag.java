package edu.harvard.iq.dataverse.api.datatag;

import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.api.Util;
import edu.harvard.iq.dataverse.engine.command.impl.PersistDataTagCommand;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Logger;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

@Path("files")//datatags already exist on DataTagsAPI
public class DataTag extends AbstractApiBean {

    private static final Logger logger = Logger.getLogger(DataTag.class.getCanonicalName());
    DataTagValidator dataTagValidator = DataTagValidator.getInstance();

    @POST
    @Path("datatags-json")
    @Consumes("application/json")
    public Response addDataTagRecord(String body, @QueryParam("id") List<Long> idsSupplied, @QueryParam("expiredDateTime") String expiredDateTime) {
        if (settingsSvc.getValueForKey(SettingsServiceBean.Key.DataTagService) == null){
            return error(FORBIDDEN, BundleUtil.getStringFromBundle("datatag.conf.disabled"));
        }
        boolean validInput = dataTagValidator.isDataTagValid(body, DataTagValidator.Type.API_BODY);
        if (!validInput)
            return error(BAD_REQUEST, BundleUtil.getStringFromBundle("datatag.api.bad.json.request"));

        JsonReader jsonReader = Json.createReader(new StringReader(body));
        JsonObject tgsJsonObject = jsonReader.readObject();
        jsonReader.close();
        if (LocalDateTime.now().isAfter(LocalDateTime.parse(expiredDateTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME)))
            return error(BAD_REQUEST, BundleUtil.getStringFromBundle("datatag.api.expired"));

        String colorCode = tgsJsonObject.getString("colorCode");
        if (!colorCode.startsWith("#"))
            return error(BAD_REQUEST, BundleUtil.getStringFromBundle("datatag.invalidSchemaError"));
        String tag = tgsJsonObject.getString("tag");

        try {
            JsonObjectBuilder jsonResponse = Json.createObjectBuilder();
            for (Long id: idsSupplied) {
                DataFile dataFile = findDataFileOrDie(String.valueOf(id));
                execCommand(new PersistDataTagCommand(createDataverseRequest(findUserOrDie()), dataFile, colorCode, tag , body, true));
                jsonResponse.add("message", BundleUtil.getStringFromBundle("datatag.saved") + " " + dataFile.getDisplayName());

            }
            return ok(jsonResponse);
        } catch (WrappedResponse ex) {
            return ex.getResponse();
        } catch (Exception e) {
            return error(BAD_REQUEST, BundleUtil.getStringFromBundle("datatag.invalidSchemaError"));
        }
    }



    
}
