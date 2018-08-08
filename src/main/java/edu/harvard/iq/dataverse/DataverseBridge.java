package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.MailUtil;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
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
import java.io.*;
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
        UNKNOWN_ERROS("UNKNOWN-ERROR"),
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
        try {
            StateEnum state = retrievePOSTResponseAsJsonObject(ingestData);
            return state;

        } catch (IOException e) {
            logger.severe(e.getMessage());
            return StateEnum.UNKNOWN_ERROS;

        }
    }

    public StateEnum checkArchivingProgress(String persistentId, String datasetVersionFriendlyNumber) {
        String state;
        logger.info("Check archiving state....");
        JsonObject jsonObjectArchived = null;
        String path = null;
        try {
            path = settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeUrl, "") + "/archive/state?srcXml="
                    + URLEncoder.encode(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) + persistentId, StandardCharsets.UTF_8.toString())
                    + "&srcVersion=" + URLEncoder.encode(datasetVersionFriendlyNumber, StandardCharsets.UTF_8.toString()) + "&targetIri=" + URLEncoder.encode(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeTdrIri), StandardCharsets.UTF_8.toString());
            jsonObjectArchived = retrieveGETResponseAsJsonObject(path);
        } catch (UnsupportedEncodingException e) {
            logger.severe(e.getMessage());

        } catch (IOException e) {
            if (e.getMessage().contains("Connection refused")) {
                //send mail to dataverseAdmin
                //authService.getAdminUser().getEmail(); we cannot use this method since it will search superuser and superuser can be more than 1

                String systemEmail = settingsService.getValueForKey(SettingsServiceBean.Key.SystemEmail);
                InternetAddress systemAddress = MailUtil.parseSystemAddress(systemEmail);
                mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), "FATAL ERROR ", e.getMessage());
                return StateEnum.BRIDGE_DOWN;
            }
        }

        if(jsonObjectArchived == null) {
            state = StateEnum.FAILED.value;
            //archivingProgressState.setFinish(true);
            updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state);
            logger.severe("Archiving is failed.");
            return StateEnum.UNKNOWN_ERROS;
        }

        state = jsonObjectArchived.getString("state", "");
        if (state.equals(StateEnum.ARCHIVED.value)) {
            //archivingProgressState.setFinish(true);
            logger.info("Update archiving state in the datasetVersion table.");
            updateDatasetVersionToArchived(persistentId, datasetVersionFriendlyNumber, jsonObjectArchived);
            //The following code is commented since it will not work: FacesMessage inside a new threads
            //addMessage(FacesMessage.SEVERITY_INFO,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.success.archived"), null);
        }else {
            logger.info("ARCHIVING state: " + state);
            updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state);
            //The following code is commented out since it will not work: FacesMessage inside a new threads
            //addMessage(FacesMessage.SEVERITY_INFO,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.failed.archived"), null);

        }
        return StateEnum.UNKNOWN_ERROS;
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
        updateDataverseVersionState(persistentId, archiveNoteState, datasetVersionFriendlyNumber);
        logger.info("Send mail to ingester");
        //send mail to ingester
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


    private StateEnum retrievePOSTResponseAsJsonObject(String jsonIngestData) throws IOException {
        String state = "";
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

                case HttpStatus.SC_FORBIDDEN:
                    return StateEnum.INVALID_USER_CREDENTIAL;

                case HttpStatus.SC_REQUEST_TIMEOUT:
                    return StateEnum.REQUEST_TIME_OUT;
            }
        } catch (IOException e) {
            if (e.getMessage().contains("Connection refused")) {
                //send mail to dataverseAdmin
                //authService.getAdminUser().getEmail(); we cannot use this method since it will search superuser and superuser can be more than 1

                String systemEmail = settingsService.getValueForKey(SettingsServiceBean.Key.SystemEmail);
                InternetAddress systemAddress = MailUtil.parseSystemAddress(systemEmail);
                mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail(), "FATAL ERROR ", e.getMessage());
                return StateEnum.BRIDGE_DOWN;
            }
        }
        return StateEnum.UNKNOWN_ERROS;

    }


    public void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        FacesContext.getCurrentInstance().addMessage(null, message);
    }


    private String readEntityAsString(HttpEntity entity) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(entity.getContent(), bos);
        return new String(bos.toByteArray(), "UTF-8");
    }


}