package edu.harvard.iq.dataverse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.authorization.users.ApiToken;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.MailUtil;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.mail.internet.InternetAddress;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
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
public class DataverseBridgeDialog implements java.io.Serializable {

    @EJB
    DatasetServiceBean datasetService;
    @EJB
    DatasetVersionServiceBean datasetVersionService;
    @EJB
    SettingsServiceBean settingsService;
    @EJB
    DataverseServiceBean dataverseService;
    @Inject
    DataverseSession dataverseSession;
    @EJB
    AuthenticationServiceBean authService;
    @EJB
    MailServiceBean mailServiceBean;


    private Logger logger = Logger.getLogger(DataverseBridgeDialog.class.getCanonicalName());
    private String datasetVersionFriendlyNumber;
    private String tdrUsername;
    private String tdrPassword;

    public String getPersistentId() {
        return persistentId;
    }

    private String persistentId;


    public void reload(String path) throws IOException {
        ExternalContext ec = FacesContext.getCurrentInstance().getExternalContext();
        ec.redirect(path);
    }


    public void ingestToTdr() {
        logger.info("INGEST TO TDR");
        try {
            DataverseBridge dbd = new DataverseBridge(settingsService, datasetService, datasetVersionService, authService, mailServiceBean);
            DataverseBridge.StateEnum state = dbd.ingestToTdr(composeJsonIngestData());
            switch (state) {
                case IN_PROGRESS:
                    Dataset dataset = datasetService.findByGlobalId(persistentId);
                    DatasetVersion dv = datasetVersionService.findByFriendlyVersionNumber(dataset.getId(), datasetVersionFriendlyNumber);
                    dv.setArchiveNote(state.toString());
                    datasetVersionService.update(dv);
                    logger.info("Archive is in process..... Please Refresh it.");
                    //checkArchivingProgress();
                    break;
                case BRIDGE_DOWN:
                    dbd.addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.bridgeserver.down"), null);
                    break;
                case TDR_DOWN:
                    dbd.addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdr.down"), null);
                    break;
                case REQUEST_TIME_OUT:
                    addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.tdr.down"), null);
                    break;

            }
        } catch (IOException e) {
            logger.severe(e.getMessage());
            addMessage(FacesMessage.SEVERITY_ERROR,BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.unknown"), null);
        }
    }
/*
    public void checkArchivingProgress() {
            //final ArchivingProgressState archivingProgressState = new ArchivingProgressState();
            Flowable.fromCallable(() -> {
                String state = DataverseBridge.STATE_IN_PROGRESS;
                while (state.equals(DataverseBridge.STATE_IN_PROGRESS)) {
                    //state = updateArchivingState();
                    if (state == null) break;
                    if (state.equals(DataverseBridge.STATE_IN_PROGRESS))
                        Thread.sleep(60000);
                }
                return state;
            })
                    .subscribeOn(Schedulers.io())
                    .observeOn(Schedulers.single())
                    .onErrorResumeNext(
                            throwable -> {
                                String es = throwable.toString();
                                logger.severe(es);
                            }
                    )
//                .doOnComplete(new Action() {
//                    @Override
//                    public void run()  {
//                            if (archivingProgressState.isFinish()){
//                                //check state, it can be failed or archived
//                                //send mail to ingester
//                                logger.info("Archiving finish.");
//                            }
//                    }
//                })
                    .subscribe();

    }*/


    public void addMessage(FacesMessage.Severity severity, String summary, String detail) {
        FacesMessage message = new FacesMessage(severity, summary, detail);
        FacesContext.getCurrentInstance().addMessage(null, message);
    }

    private String composeJsonIngestData() throws JsonProcessingException {
        SrcData srcData = new SrcData(settingsService.getValueForKey(SettingsServiceBean.Key.DataverseDdiExportBaseURL) + persistentId, datasetVersionFriendlyNumber, getApiTokenKey());

        TdrData tdrData = new TdrData(tdrUsername, tdrPassword, settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeTdrIri));
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(new IngestData(srcData, tdrData));
    }

    private String readEntityAsString(HttpEntity entity) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(entity.getContent(), bos);
        return new String(bos.toByteArray(), "UTF-8");
    }

    /* THIS IS TworRavensHelper Code, we should make it public on th TwoRavensHelper class and use it here*/
    private String getApiTokenKey() {
        ApiToken apiToken;
        if (dataverseSession.getUser() == null) {
            return null;
        }
        if (dataverseSession.getUser().isAuthenticated()) {
            AuthenticatedUser au = (AuthenticatedUser) dataverseSession.getUser();
            apiToken = authService.findApiTokenByUser(au);
            if (apiToken != null) {
                return apiToken.getTokenString();
            }
            // Generate if not available?
            // Or should it just be generated inside the authService
            // automatically?
            apiToken = authService.generateApiTokenForUser(au);
            if (apiToken != null) {
                return apiToken.getTokenString();
            }
        }
        return "";
    }


    public String getTdrUsername() {
        return tdrUsername;
    }

    public void setTdrUsername(String tdrUsername) {
        this.tdrUsername = tdrUsername;
    }

    public String getTdrPassword() {
        return tdrPassword;
    }

    public void setTdrPassword(String tdrPassword) {
        this.tdrPassword = tdrPassword;
    }


    public void setDatasetVersionFriendlyNumber(String datasetVersionFriendlyNumber) {
        this.datasetVersionFriendlyNumber = datasetVersionFriendlyNumber;
    }


    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }

    private class SrcData {
        private String srcXml;
        private String srcVersion;
        private String appName = "DATAVERSE";
        private String apiToken;

        public SrcData(String srcXml, String srcVersion, String apiToken) {
            this.srcXml = srcXml;
            this.srcVersion = srcVersion;
            this.apiToken = apiToken;
        }

        public String getSrcXml() {
            return srcXml;
        }

        public String getSrcVersion() {
            return srcVersion;
        }

        public String getAppName() {
            return appName;
        }

        public String getApiToken() {
            return apiToken;
        }
    }
    private class TdrData {
        private String username;
        private String password;
        private String iri;
        private String appName = "EASY";

        public TdrData(String username, String password, String iri) {
            this.username = username;
            this.password = password;
            this.iri = iri;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getIri() {
            return iri;
        }

        public String getAppName() {
            return appName;
        }
    }
    private class IngestData{
        private SrcData srcData;
        private TdrData tdrData;

        public IngestData(SrcData srcData, TdrData tdrData) {
            this.srcData = srcData;
            this.tdrData = tdrData;
        }

        public SrcData getSrcData() {
            return srcData;
        }

        public TdrData getTdrData() {
            return tdrData;
        }
    }

}