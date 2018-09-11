package edu.harvard.iq.dataverse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.authorization.users.ApiToken;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 *
 * @author Eko Indarto
 */
@ViewScoped
@Named
public class DataverseBridgeDialog implements java.io.Serializable {

    @EJB
    private DatasetServiceBean datasetService;
    @EJB
    private DatasetVersionServiceBean datasetVersionService;
    @EJB
    private SettingsServiceBean settingsService;
    @EJB
    private DataverseServiceBean dataverseService;
    @Inject
    private DataverseSession session;
    @EJB
    private AuthenticationServiceBean authService;
    @EJB
    private MailServiceBean mailServiceBean;


    private Logger logger = Logger.getLogger(DataverseBridgeDialog.class.getCanonicalName());
    private String datasetVersionFriendlyNumber;
    private String darUsername;
    private String darPassword;
    private Map<String, String> dvDarConfs = new HashMap<>();
    private String darName = "EASY";
    private List<String> darNames;
    private String persistentId;
    private DataverseBridge dataverseBridge;

    @PostConstruct
    public void init() {
        if (session.getUser().isAuthenticated() && settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeConf) != null) {
            dataverseBridge =  new DataverseBridge(((AuthenticatedUser) session.getUser()).getEmail(), settingsService, datasetService, datasetVersionService, authService, mailServiceBean);
            dvDarConfs = dataverseBridge.getDvBridgeConf().getConf();
            darNames = new ArrayList<>(dvDarConfs.keySet());
        }
    }

    public void archive() {
        logger.info("INGEST TO Digital Archive Repository");

        AuthenticatedUser au = (AuthenticatedUser) session.getUser();
        logger.info(" The user '" + au.getIdentifier() + "' is trying to archive '" + persistentId + "' to '" + darName + "'.");
        DataverseBridge.StateEnum state;
        String dvBaseMetadataXml = dvDarConfs.get(darName);
        String jsonIngestData = composeJsonIngestData(dvBaseMetadataXml, darName);
        if (jsonIngestData != null) {
            JsonObject postResponseJsonObject = dataverseBridge.ingestToDar(jsonIngestData);
            state = DataverseBridge.StateEnum.fromValue(postResponseJsonObject.getString("state"));
            if (state == DataverseBridge.StateEnum.IN_PROGRESS) {
                dataverseBridge.updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.toString() + "@" + darName);
                Flowable.fromCallable(() -> {
                    DataverseBridge.StateEnum currentState = DataverseBridge.StateEnum.IN_PROGRESS;
                    int hopCount = 0;
                    while ((currentState == DataverseBridge.StateEnum.IN_PROGRESS) && hopCount < 10) {
                        Thread.sleep(600000);//10 minutes
                        hopCount += 1;
                        logger.info(".... Checking Archiving Progress of " + persistentId + ".....[" + hopCount + "]");
                        currentState = dataverseBridge.checkArchivingProgress(dvBaseMetadataXml, persistentId, datasetVersionFriendlyNumber, darName);
                    }
                    logger.info("Hop count: " + hopCount + "\t Current state of '" + persistentId + "': " + currentState);
                    return currentState;
                })
                        .subscribeOn(Schedulers.io())
                        .observeOn(Schedulers.single())
                        .doOnError(
                                throwable -> {
                                    String es = throwable.toString();
                                    logger.severe(es);
                                }
                        )
                        .subscribe(cs -> logger.info("The archiving process of dataset of '" + persistentId + "' with version: " + datasetVersionFriendlyNumber + " is done."),
                                throwable -> logger.severe(throwable.getMessage()));

                FacesMessage message = new FacesMessage(FacesMessage.SEVERITY_INFO
                        , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress")
                        , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress.detail"));
                FacesContext.getCurrentInstance().addMessage(null, message);
            } else {
                logger.finest("persistentId: " + persistentId + "\tdatasetversion: " + datasetVersionFriendlyNumber + " has state: " + state);
                dataverseBridge.updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
            }
        } else {
            logger.severe("Failed to compose ingest data.");
            mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail()
                    , "Failed to compose ingest data.", "persistentId: " + persistentId + "\nVersion: " + datasetVersionFriendlyNumber );
            FacesMessage message = new FacesMessage(FacesMessage.SEVERITY_ERROR
                    , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.transfer")
                    , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.error.unknown"));
            FacesContext.getCurrentInstance().addMessage(null, message);
        }
    }

    private String composeJsonIngestData(String dvBaseMetadataXml, String darName){
        SrcData srcData = new SrcData(dvBaseMetadataXml + persistentId, datasetVersionFriendlyNumber, getApiTokenKey());
        DarData darData = new DarData(darName, darUsername, darPassword);
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(new IngestData(srcData, darData));
        } catch (JsonProcessingException e) {
            mailServiceBean.sendSystemEmail(authService.getAuthenticatedUser("dataverseAdmin").getEmail()
                    , "JsonProcessingException", "dvBaseMetadataXml: " + dvBaseMetadataXml + "\ndarName: " + darName
                            +"\nJsonProcessingException, msg: " + e.getMessage());
        }
        return null;
    }

    private String getApiTokenKey() {
        //No need user auth check since this class will be only accessed by auth user.
        ApiToken apiToken;
        AuthenticatedUser au = (AuthenticatedUser) session.getUser();
        apiToken = authService.findApiTokenByUser(au);
        if (apiToken != null) {
            //check whether still valid or already expired/disable.
            AuthenticatedUser aux = authService.lookupUser(apiToken.getTokenString());
            if (aux == null) {
                //disable or expired token, so generate one.
                apiToken = authService.generateApiTokenForUser(au);
                if (apiToken != null) {
                    return apiToken.getTokenString();
                } else
                    return "";
            } else
                return apiToken.getTokenString();
        }
        // Generate if not available?
        // Or should it just be generated inside the authService
        // automatically?
        apiToken = authService.generateApiTokenForUser(au);
        if (apiToken != null) {
            return apiToken.getTokenString();
        }
        return "";
    }


    public String getDarUsername() {
        return darUsername;
    }

    public void setDarUsername(String darUsername) {
        this.darUsername = darUsername;
    }

    public String getDarPassword() {
        return darPassword;
    }

    public void setDarPassword(String darPassword) {
        this.darPassword = darPassword;
    }


    public void setDatasetVersionFriendlyNumber(String datasetVersionFriendlyNumber) {
        this.datasetVersionFriendlyNumber = datasetVersionFriendlyNumber;
    }

    public String getPersistentId() {
        return persistentId;
    }

    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }


    public void setDarNames(Map<String, String> dvDarConfs) {
        this.dvDarConfs = dvDarConfs;
    }

    public String getDarName() {
        return darName;
    }

    public void setDarName(String darName) {
        this.darName = darName;
    }

    public List<String> getDarNames() {
        return darNames;
    }



    private class SrcData {
        private String srcXml;
        private String srcVersion;
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

        public String getApiToken() {
            return apiToken;
        }
    }
    private class DarData {
        private String username;
        private String password;
        private String darName;
        public DarData(String darName, String username, String password) {
            this.darName = darName;
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getDarName() {
            return darName;
        }
    }
    private class IngestData{
        private SrcData srcData;
        private DarData darData;

        public IngestData(SrcData srcData, DarData darData) {
            this.srcData = srcData;
            this.darData = darData;
        }

        public SrcData getSrcData() {
            return srcData;
        }

        public DarData getDarData() {
            return darData;
        }
    }

}