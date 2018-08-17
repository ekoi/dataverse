package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.MailUtil;
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
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.mail.internet.InternetAddress;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

/**
 *
 * @author Eko Indarto
 */
@ViewScoped
@Named
public class DataverseBridge implements java.io.Serializable {

    DatasetServiceBean datasetService;
    DatasetVersionServiceBean datasetVersionService;
    SettingsServiceBean settingsService;
    DataverseServiceBean dataverseService;
    AuthenticationServiceBean authService;
    MailServiceBean mailServiceBean;


    private Logger logger = Logger.getLogger(DataverseBridge.class.getCanonicalName());

    public enum StateEnum {
        IN_PROGRESS("IN-PROGRESS"),
        ERROR("ERROR"),
        FAILED("FAILED"),
        ARCHIVED("ARCHIVED"),
        REJECTED("REJECTED"),
        INVALID("INVALID"),
        TDR_DOWN("TDR-DOWN"),
        INVALID_USER_CREDENTIAL("INVALID-USER-CREDENTIAL"),
        REQUEST_TIME_OUT("REQUEST-TIME-OUT"),
        UNKNOWN_ERROR("UNKNOWN-ERROR"),
        BRIDGE_DOWN("BRIDGE-DOWN");
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

    public DataverseBridge(SettingsServiceBean settingsService, DatasetServiceBean datasetService, DatasetVersionServiceBean datasetVersionService, AuthenticationServiceBean authService, MailServiceBean mailServiceBean){
        this.settingsService = settingsService;
        this.datasetService = datasetService;
        this.datasetVersionService = datasetVersionService;
        this.authService = authService;
        this.mailServiceBean = mailServiceBean;
    }

    public StateEnum ingestToTdr(String ingestData) {
        logger.info("INGEST TO TDR");
        return retrievePOSTResponseAsJsonObject(ingestData);
    }

    public StateEnum checkArchivingProgress(String persistentId, String datasetVersionFriendlyNumber) {
        StateEnum state = StateEnum.UNKNOWN_ERROR;
        logger.info("Check archiving state....");
        JsonObject jsonObjectArchived = null;
        String path = null;
        try {
            path = settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeUrl, "") + "/archive/state?srcXml="
                    + URLEncoder.encode(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) + persistentId, StandardCharsets.UTF_8.toString())
                    + "&srcVersion=" + URLEncoder.encode(datasetVersionFriendlyNumber, StandardCharsets.UTF_8.toString()) + "&targetIri=" + URLEncoder.encode(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeTdrIri), StandardCharsets.UTF_8.toString());
            jsonObjectArchived = retrieveGETResponseAsJsonObject(path);
            if (jsonObjectArchived != null) {
               state = StateEnum.fromValue(jsonObjectArchived.getString("state", "UNKNOWN-ERROR"));
               if (state == StateEnum.ARCHIVED) {
                    logger.info("Update archiving state in the datasetVersion table.");
                    updateDatasetVersionToArchived(persistentId, datasetVersionFriendlyNumber, jsonObjectArchived);
                }
            }

        } catch (UnsupportedEncodingException e) {
            logger.severe(e.getMessage());
            state = StateEnum.UNKNOWN_ERROR;
        } catch (IOException e) {
            if (e.getMessage().contains("Connection refused"))
                state = StateEnum.BRIDGE_DOWN;
        }


        if (state != StateEnum.ARCHIVED) {
            logger.info("ARCHIVING state: " + state);
            updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
        }
        return state;
    }

    private void sendMail(String subject, String msg) {
        mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), subject, msg);
        logger.severe(msg);
    }

    public void updateArchivenoteAndDisplayMessage(String persistentId, String datasetVersionFriendlyNumber, StateEnum state) {
        String msg = "";
        switch (state) {
            case BRIDGE_DOWN:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.bridgeserver.down");
                break;
            case TDR_DOWN:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdr.down");
                break;
            case REQUEST_TIME_OUT:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdr.down");
                break;
            case INVALID_USER_CREDENTIAL:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdrcredentias");
                break;
            case REJECTED:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdr.rejected");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case IN_PROGRESS:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case INVALID:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdr.invalid");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case ERROR:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.tdr.error");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            case UNKNOWN_ERROR:
                msg = BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.unknown");
                updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.value);
                break;
            default: //do nothing
        }

        if (state != StateEnum.IN_PROGRESS) {
            sendMail(state.value, msg);
            addMessage(FacesMessage.SEVERITY_ERROR, msg,null);
        }
//        else
//            addMessage(FacesMessage.SEVERITY_INFO, msg,null);
    }

    private void updateDataverseVersionState(String persistentId, String datasetVersionFriendlyNumber, String state) {
        Dataset dataset = datasetService.findByGlobalId(persistentId);
        DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
        dv.setArchiveTime(new Date());
        dv.setArchiveNote(state);
        datasetVersionService.update(dv);
    }

    private void updateDatasetVersionToArchived(String persistentId, String datasetVersionFriendlyNumber, JsonObject jsonObjectArchived) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMMM d, yyyy");
        String archiveNoteState = jsonObjectArchived.getString("landingPage", "") + "#" +
                jsonObjectArchived.getString("doi", "") +
                "#" + simpleDateFormat.format(new Date());
        logger.info(archiveNoteState);
        updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, archiveNoteState);
        //logger.info("Send mail to ingester");
        //todo:send mail to ingester(?)
    }

    private JsonObject retrieveGETResponseAsJsonObject(String path) throws IOException {
        JsonObject jsonObject = null;
        JsonReader reader = null;
        //see https://stackoverflow.com/questions/21574478/what-is-the-difference-between-closeablehttpclient-and-httpclient-in-apache-http
        try(CloseableHttpClient httpclient = HttpClients.createDefault()){
            HttpGet httpGet = new HttpGet(path);
            httpGet.addHeader("accept", "application/json");
            CloseableHttpResponse response = httpclient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                reader = Json.createReader(new InputStreamReader((response.getEntity().getContent())));
                jsonObject = reader.readObject();
                reader.close();
            }
        }
        return jsonObject;
    }


    private StateEnum retrievePOSTResponseAsJsonObject(String jsonIngestData){
         try (CloseableHttpClient httpClient = HttpClients.createDefault()){
            HttpPost httpPost = new HttpPost(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeUrl, "") + "/archive/create");
            logger.finest("json that send to dataverse-bridge server (/archive/create):  " + jsonIngestData);
            StringEntity entity = new StringEntity(jsonIngestData);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            switch (httpClient.execute(httpPost).getStatusLine().getStatusCode()) {
                case HttpStatus.SC_CREATED:
                    return StateEnum.IN_PROGRESS;
                case HttpStatus.SC_OK:
                    return StateEnum.IN_PROGRESS;
                case HttpStatus.SC_FORBIDDEN:
                    return StateEnum.INVALID_USER_CREDENTIAL;
                case HttpStatus.SC_REQUEST_TIMEOUT:
                    return StateEnum.REQUEST_TIME_OUT;
            }
        } catch (IOException e) {
            if (e.getMessage().contains("Connection refused")) {
                String systemEmail = settingsService.getValueForKey(SettingsServiceBean.Key.SystemEmail);
                InternetAddress systemAddress = MailUtil.parseSystemAddress(systemEmail);
                mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), "FATAL ERROR ", e.getMessage());
                return StateEnum.BRIDGE_DOWN;
            }
        }
        return StateEnum.UNKNOWN_ERROR;

    }


    public void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        FacesContext.getCurrentInstance().addMessage(null, message);
    }
}