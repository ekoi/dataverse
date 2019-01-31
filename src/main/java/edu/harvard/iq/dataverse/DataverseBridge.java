package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.MailUtil;
import edu.harvard.iq.dataverse.util.SystemConfig;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.mail.internet.InternetAddress;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author Eko Indarto
 */
@ViewScoped
@Named
public class DataverseBridge implements java.io.Serializable {

    private DatasetServiceBean datasetService;
    private DatasetVersionServiceBean datasetVersionService;
    private SettingsServiceBean settingsService;
    private AuthenticationServiceBean authService;
    private MailServiceBean mailServiceBean;
    private DataverseBridgeSetting dataverseBridgeSetting;
    private String userMail;

    private Logger logger = Logger.getLogger(DataverseBridge.class.getCanonicalName());
    private static String RESPONSE_STATE = "{ \"state\":\"value\" }";

    public enum StateEnum {
        IN_PROGRESS("IN-PROGRESS"),
        ERROR("ERROR"),
        FAILED("FAILED"),
        ARCHIVED("ARCHIVED"),
        REJECTED("REJECTED"),
        INVALID("INVALID"),
        DAR_DOWN("DAR-DOWN"),
        BRIDGE_DOWN("BRIDGE-DOWN"),
        INVALID_USER_CREDENTIAL("INVALID_USER_CREDENTIAL"),
        REQUEST_TIME_OUT("REQUEST_TIME_OUT"),
        INTERNAL_SERVER_ERROR("INTERNAL_SERVER_ERROR");
        private String value;

        StateEnum(String value) {
            this.value = value;
        }

        public String toString() {
            return String.valueOf(value);
        }

        public static StateEnum fromValue(String text) {
            for (StateEnum b : StateEnum.values()) {
                if (String.valueOf(b.value).equals(text)) {
                    return b;
                }
            }
            return null;
        }
    }

    public DataverseBridge(String userMail, SettingsServiceBean settingsService, DatasetServiceBean datasetService, DatasetVersionServiceBean datasetVersionService, AuthenticationServiceBean authService, MailServiceBean mailServiceBean) {
        this.userMail = userMail;
        this.settingsService = settingsService;
        this.datasetService = datasetService;
        this.datasetVersionService = datasetVersionService;
        this.authService = authService;
        this.mailServiceBean = mailServiceBean;
        this.dataverseBridgeSetting = getDvBridgeConf(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeConf));
    }

    public JsonObject ingestToDar(String ingestData, boolean skipDarAuthPreCheck) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(dataverseBridgeSetting.dvnSettingBridge.url + "/archiving");
            logger.finest("json that send to dataverse-bridge server (/archiving):  " + ingestData);
            StringEntity entity = new StringEntity(ingestData);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            httpPost.setHeader("api_key", dataverseBridgeSetting.dvnSettingBridge.apiKey);
            httpPost.setHeader("skipDarAuthPreCheck", Boolean.toString(skipDarAuthPreCheck));
            CloseableHttpResponse response = httpClient.execute(httpPost);
            switch (response.getStatusLine().getStatusCode()) {
                case HttpStatus.SC_CREATED:
                case HttpStatus.SC_OK:
                    JsonReader readerInprogres = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.IN_PROGRESS.value)));
                    JsonObject jsonObject = readerInprogres.readObject();
                    readerInprogres.close();
                    return jsonObject;
                case HttpStatus.SC_FORBIDDEN:
                    JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.INVALID_USER_CREDENTIAL.value)));
                    JsonObject jo = jsonReader.readObject();
                    jsonReader.close();
                    return jo;
                case HttpStatus.SC_REQUEST_TIMEOUT:
                    JsonReader readerDarDown = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.DAR_DOWN.value)));
                    JsonObject darDownJsonObject = readerDarDown.readObject();
                    readerDarDown.close();
                    return darDownJsonObject;
            }
        } catch (IOException e) {
            logger.severe(e.getMessage());
            if (e.getMessage().contains("Connection refused")) {
                JsonObject responseJsonObject = reportBridgeDown(e);
                return responseJsonObject;
            }
            mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail()
                    , "IOException", ingestData + "\nUnsupportedEncodingException, msg: " + e.getMessage());
        }

        JsonReader reader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.INTERNAL_SERVER_ERROR.value)));
        JsonObject responseJsonObject = reader.readObject();
        reader.close();
        return responseJsonObject;
    }

    public StateEnum checkArchivingProgress(String dvBaseMetadataXml, String persistentId, String datasetVersionFriendlyNumber, String darName) {
        StateEnum state = StateEnum.INTERNAL_SERVER_ERROR;
        logger.info("... Check archiving state of '" + persistentId + "' v: " + datasetVersionFriendlyNumber);
        JsonObject jsonObjectReponse;
        String path;
        try {
            path = dataverseBridgeSetting.dvnSettingBridge.url + "/archiving/state?srcMetadataUrl="
                    + URLEncoder.encode(dvBaseMetadataXml + persistentId, StandardCharsets.UTF_8.toString())
                    + "&srcMetadataVersion=" + URLEncoder.encode(datasetVersionFriendlyNumber, StandardCharsets.UTF_8.toString()) + "&targetDarName=" + darName;
            jsonObjectReponse = retrieveGETResponseAsJsonObject(path);
            state = StateEnum.fromValue(jsonObjectReponse.getString("state", "UNKNOWN-ERROR"));
            logger.info("ARCHIVING state: " + state);
            if (state == StateEnum.ARCHIVED) {
                logger.info("Update archiving state in the datasetVersion table.");
                updateDatasetVersionToArchived(persistentId, datasetVersionFriendlyNumber, jsonObjectReponse);
            } else
                updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
        } catch (UnsupportedEncodingException e) {
            logger.severe(e.getMessage());
            mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail()
                    , "UnsupportedEncodingException", "persistentId: " + persistentId + "\nVersion: "
                            + datasetVersionFriendlyNumber + "\nUnsupportedEncodingException, msg: " + e.getMessage());
        }
        logger.info(persistentId + " v: " + datasetVersionFriendlyNumber + " has state: " + state);
        return state;
    }

    public void updateArchivenoteAndDisplayMessage(String persistentId, String datasetVersionFriendlyNumber, StateEnum state) {
        String displayMessage = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.transfer");
        String displayMessageDetails = "";
        String emailSubject = state.value + " on persistentId: " + persistentId + " Version: " + datasetVersionFriendlyNumber;
        String emailMessage = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.email",
                Arrays.asList(SystemConfig.getDataverseSiteUrlStatic(), persistentId, datasetVersionFriendlyNumber));
        boolean sendMailToAdmin = true;
        FacesMessage.Severity fs = FacesMessage.SEVERITY_ERROR;
        switch (state) {
            case BRIDGE_DOWN:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.bridgeserver.down");
                emailMessage = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.bridgeserver.down.email");
                emailSubject = state.value;
                break;
            case DAR_DOWN:
            case REQUEST_TIME_OUT:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.dar.down");
                emailMessage = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.dar.down.email");
                emailSubject = state.value;
                break;
            case INVALID_USER_CREDENTIAL:
                sendMailToAdmin = false;
                displayMessage = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.credentials");
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.credentials.detail");
                break;
            case FAILED:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.failed");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case REJECTED:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.rejected");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case IN_PROGRESS:
                sendMailToAdmin = false;
                fs = FacesMessage.SEVERITY_INFO;
                displayMessage = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress");
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress.detail");
                break;
            case INVALID:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.invalid");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case ERROR:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.unknown");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case INTERNAL_SERVER_ERROR:
                displayMessageDetails = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.unknown");
                break;
        }
        addMessage(fs, displayMessage, displayMessageDetails);
        if (sendMailToAdmin) {
            mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), emailSubject, emailMessage);
        }
    }

    public String getDataverseVersionNoteText(String persistentId, String datasetVersionFriendlyNumber) {
        Dataset dataset = datasetService.findByGlobalId(persistentId);
        if (dataset != null) {
            DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
            return dv.getDarNote();
        }
        return null;
    }

    public void updateDataverseVersionState(String persistentId, String datasetVersionFriendlyNumber, String state) {
        Dataset dataset = datasetService.findByGlobalId(persistentId);
        DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
        dv.setArchiveTime(new Date());
        dv.setDarNote(state);
        datasetVersionService.update(dv);
    }

    private void updateDatasetVersionToArchived(String persistentId, String datasetVersionFriendlyNumber, JsonObject jsonObjectArchived) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMMM d, yyyy");
        String pid = jsonObjectArchived.getString("pid", "");
        String archiveNoteState = jsonObjectArchived.getString("landingPage", "") + "#" +
                pid +
                "#" + simpleDateFormat.format(new Date());
        logger.info(archiveNoteState);
        String currentArchiveNote = getDataverseVersionNoteText(persistentId, datasetVersionFriendlyNumber);
        if (currentArchiveNote != null && !currentArchiveNote.contains(pid)) {
            updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, archiveNoteState);
            String msg = BundleUtil.getStringFromBundle("dataset.archive.notification.email.archiving.finish.text", Arrays.asList(persistentId, pid));
            mailServiceBean.sendSystemEmail(userMail, BundleUtil.getStringFromBundle("dataset.archive.notification.email.archiving.finish.subject"), msg);
            logger.info("Mail is send to: " + userMail);
        } else {
            logger.info("Archive note is already updated, with text: " + archiveNoteState);
        }
    }

    private JsonObject retrieveGETResponseAsJsonObject(String path) {
        //see https://stackoverflow.com/questions/21574478/what-is-the-difference-between-closeablehttpclient-and-httpclient-in-apache-http
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(path);
            httpGet.addHeader("accept", "application/json");
            CloseableHttpResponse response = httpclient.execute(httpGet);
            switch (response.getStatusLine().getStatusCode()) {
                case HttpStatus.SC_OK:
                    JsonReader reader = Json.createReader(new InputStreamReader((response.getEntity().getContent())));
                    JsonObject jsonObject = reader.readObject();
                    reader.close();
                    return jsonObject;
                case HttpStatus.SC_REQUEST_TIMEOUT:
                    JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.DAR_DOWN.value)));
                    JsonObject responseJsonObject = jsonReader.readObject();
                    jsonReader.close();
                    return responseJsonObject;
            }
        } catch (IOException e) {
            logger.severe("Error is occurred for HttpGet of " + path + ". Error message: " + e.getMessage());
            if (e.getMessage().contains("Connection refused")) {
                JsonObject responseJsonObject = reportBridgeDown(e); //send email to dataverseAdmin
                return responseJsonObject;
            } else
                mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail()
                        , "IOException", path + "\nIOException, msg: " + e.getMessage());
        }
        JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.INTERNAL_SERVER_ERROR.value)));
        JsonObject responseJsonObject = jsonReader.readObject();
        jsonReader.close();
        return responseJsonObject;
    }

    private JsonObject reportBridgeDown(IOException e) {
        String systemEmail = settingsService.getValueForKey(SettingsServiceBean.Key.SystemEmail);
        InternetAddress systemAddress = MailUtil.parseSystemAddress(systemEmail);
        mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), "ERROR - BRIDGE is DOWN! ", e.getMessage());
        JsonReader jsonReader = Json.createReader(new StringReader(RESPONSE_STATE.replace("value", StateEnum.BRIDGE_DOWN.value)));
        JsonObject responseJsonObject = jsonReader.readObject();
        jsonReader.close();
        logger.info("Bridge is down. Send email to " + authService.getAuthenticatedUser("dataverseAdmin").getEmail());
        return responseJsonObject;
    }

    private void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        if (FacesContext.getCurrentInstance() != null) //we need this check since there is no contex any more when this method is executed by a thread that run in background; eq. Flowable.fromCallable... (DataverseBridgeDialog)
            FacesContext.getCurrentInstance().addMessage(null, message);
    }

    /*
   Setting table.
   Name: :DataverseBridgeConf
   Content:
   {
    "source-name": "dataverse",
    "metadata-url": "http://ddvn.dans.knaw.nl:8080/api/datasets/export?exporter=dataverse_json&persistentId=",
    "bridge": {
        "url": "http://10.0.2.2:8592/api/v1",
        "api-key": "@km1D3cember2oo4"
    },
    "dar": [{
        "dar-name": "EASY",
        "users": [
            {
                "group-name": "SWORD",
                "dar-user-name": "user001",
                "dar-user-password": "user001",
                "dar-user-affiliation": "UVT"
            },
            {
                "group-name": "SWORD-Tilburg",
                "dar-user-name": "user002",
                "dar-user-password": "user002",
                "dar-user-affiliation": "TUE"
            }
        ]
    }]
}
  * */
    private DataverseBridgeSetting getDvBridgeConf(String dvBridgeSetting) {
        JsonReader jsonReader = Json.createReader(new StringReader(dvBridgeSetting));
        JsonObject dvBridgeSettingJsonObject = jsonReader.readObject();
        jsonReader.close();
        return new DataverseBridgeSetting(dvBridgeSettingJsonObject);
    }

    public DataverseBridgeSetting getDataverseBridgeSetting() {
        return dataverseBridgeSetting;
    }

    public class DataverseBridgeSetting {
        private String sourceName;
        private String metadataUrl;
        private DvnSettingBridge dvnSettingBridge;
        private List<DarSetting> darSettings;
        private List<String> darNames;

        DataverseBridgeSetting(JsonObject jo) {
            this.sourceName = jo.getString("source-name");
            this.metadataUrl = jo.getString("metadata-url");
            this.dvnSettingBridge = new DvnSettingBridge(jo.getJsonObject("bridge"));
            JsonArray ja = jo.getJsonArray("dar");
            darSettings = ja.stream().map(json -> new DarSetting(((JsonObject) json).getString("dar-name"),
                    ((JsonArray) ((JsonObject) json).getJsonArray("users")).stream()
                            .map(js -> new DarUser(((JsonObject) js).getString("group-name"), ((JsonObject) js).getString("dar-user-name"), ((JsonObject) js).getString("dar-user-password"), ((JsonObject) js).getString("dar-user-affiliation")))
                            .collect(Collectors.toList())))
                    .collect(Collectors.toList());
            darNames = darSettings.stream().map(j -> j.darName).collect(Collectors.toList());
        }

        public String getMetadataUrl() {
            return metadataUrl;
        }

        public DvnSettingBridge getDvnSettingBridge() {
            return dvnSettingBridge;
        }

        public String getSourceName() {
            return sourceName;
        }

        public List<DarSetting> getDarSettings() {
            return darSettings;
        }

        public List<String> getDarNames() {
            return darNames;
        }
    }

    class DvnSettingBridge {
        private String url;
        private String apiKey;

        DvnSettingBridge(JsonObject jo) {
            this.url = jo.getString("url");
            this.apiKey = jo.getString("api-key");
        }
    }

    class DarSetting {
        private String darName;
        private List<DarUser> darUsers;
        private List<String> userGroups;

        public DarSetting(String darName, List<DarUser> darUsers) {
            this.darName = darName;
            this.darUsers = darUsers;
            userGroups = darUsers.stream().map(x -> x.getGroupName()).collect(Collectors.toList());
        }

        public String getDarName() {
            return darName;
        }

        public List<DarUser> getDarUsers() {
            return darUsers;
        }

        public List<String> getUserGroups() {
            return userGroups;
        }
    }

    class DarUser {
        private String groupName;
        private String darUsername;
        private String darPassword;
        private String darUsernameAffiliation;

        DarUser(String groupName, String darUsername, String darPassword, String darUsernameAffiliation) {
            this.groupName = groupName;
            this.darUsername = darUsername;
            this.darPassword = darPassword;
            this.darUsernameAffiliation = darUsernameAffiliation;
        }

        public String getGroupName() {
            return groupName;
        }

        public String getDarUsername() {
            return darUsername;
        }

        public String getDarPassword() {
            return darPassword;
        }

        public String getDarUsernameAffiliation() {
            return darUsernameAffiliation;
        }
    }
}

