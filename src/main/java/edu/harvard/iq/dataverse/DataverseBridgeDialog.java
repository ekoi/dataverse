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
import java.io.IOException;
import java.util.List;
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
    private String darUserAffiliation;
    private DataverseBridge.DataverseBridgeSetting dataverseBridgeSetting;
    private String darName;
    private List<String> darNameList;
    private String persistentId;
    private String swordGroupAlias;
    private DataverseBridge dataverseBridge;
    private boolean displayInputCredentials;

    @PostConstruct
    public void init() {
        if (session.getUser().isAuthenticated() && settingsService.getValueForKey(SettingsServiceBean.Key.DataverseBridgeConf) != null) {
            dataverseBridge =  new DataverseBridge(((AuthenticatedUser) session.getUser()).getEmail(), settingsService, datasetService, datasetVersionService, authService, mailServiceBean);
            dataverseBridgeSetting = dataverseBridge.getDataverseBridgeSetting();
            darNameList= dataverseBridgeSetting.getDarNames();
        }
    }

    public void archive() {
        logger.info("INGEST TO Digital Archive Repository");

        AuthenticatedUser au = (AuthenticatedUser) session.getUser();
        logger.info(" The user '" + au.getIdentifier() + "' is trying to archive '" + persistentId + "' to '" + darName + "'.");
        DataverseBridge.StateEnum state;
        String dvBaseMetadataUrl = dataverseBridgeSetting.getMetadataUrl();
        try {
            JsonObject postResponseJsonObject = dataverseBridge.ingestToDar(composeJsonIngestData(dvBaseMetadataUrl, darName), !displayInputCredentials);
            state = DataverseBridge.StateEnum.fromValue(postResponseJsonObject.getString("state"));
        } catch (IOException e) {
            logger.severe(e.getMessage());
            state = DataverseBridge.StateEnum.INTERNAL_SERVER_ERROR;
        }
        if (state == DataverseBridge.StateEnum.IN_PROGRESS) {
            dataverseBridge.updateDataverseVersionState(persistentId, datasetVersionFriendlyNumber, state.toString() + "@" + darName);
            Flowable.fromCallable(() -> {
                DataverseBridge.StateEnum currentState = DataverseBridge.StateEnum.IN_PROGRESS;
                int hopCount = 0;
                while ((currentState == DataverseBridge.StateEnum.IN_PROGRESS) && hopCount < 10) {
                    Thread.sleep(900000);//15 minutes
                    hopCount += 1;
                    logger.info(".... Checking Archiving Progress of " + persistentId + ".....[" + hopCount + "]");
                    currentState = dataverseBridge.checkArchivingProgress(dvBaseMetadataUrl, persistentId, datasetVersionFriendlyNumber, darName);
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
                    .subscribe(cs -> logger.info("The dataset of '" + persistentId + "' has been archived."),
                            throwable -> logger.severe(throwable.getMessage()));

            FacesMessage message = new FacesMessage(FacesMessage.SEVERITY_INFO
                    , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress")
                    , BundleUtil.getStringFromBundle("dataset.archive.dialog.message.archiving.inprogress.detail"));
            FacesContext.getCurrentInstance().addMessage(null, message);
        } else {
            dataverseBridge.updateArchivenoteAndDisplayMessage(persistentId, datasetVersionFriendlyNumber, state);
        }

    }

    private String composeJsonIngestData(String dvBaseMetadataUrl, String darName) throws JsonProcessingException {
        SrcData srcData = new SrcData(dvBaseMetadataUrl + persistentId, datasetVersionFriendlyNumber, getApiTokenKey(), dataverseBridgeSetting.getSourceName());
        DarData darData= new DarData(darName, darUsername, darPassword, darUserAffiliation);
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(new IngestData(srcData, darData));
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

    public String getDarUserAffiliation() {
        return darUserAffiliation;
    }

    public void setDarUserAffiliation(String darUserAffiliation) {
        this.darUserAffiliation = darUserAffiliation;
    }

    public String getDatasetVersionFriendlyNumber() {
        return datasetVersionFriendlyNumber;
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

    public String getSwordGroupAlias() {
        return swordGroupAlias;
    }

    public void setSwordGroupAlias(String swordGroupAlias) {
        this.swordGroupAlias = swordGroupAlias;
    }

    public String getDarName() {
        return darName;
    }

    public void setDarName(String darName) {
        this.darName = darName;
    }

    public List<String> getDarNameList() {
        return darNameList;
    }

    public void setDarNameList(List<String> darNameList) {
        this.darNameList = darNameList;
    }

    public boolean isDisplayInputCredentials() {
        return displayInputCredentials;
    }

    public void setDisplayInputCredentials(boolean displayInputCredentials) {
        this.displayInputCredentials = displayInputCredentials;
    }
    private class SrcData {
        private String srcMetadataUrl;
        private String srcMetadataVersion;
        private String srcName;
        private String srcApiToken;

        public SrcData(String srcMetadataUrl, String srcMetadataVersion, String apiToken, String srcName) {
            this.srcMetadataUrl = srcMetadataUrl;
            this.srcMetadataVersion = srcMetadataVersion;
            this.srcApiToken = apiToken;
            this.srcName = srcName;
        }

        public String getSrcMetadataUrl() {
            return srcMetadataUrl;
        }

        public String getSrcMetadataVersion() {
            return srcMetadataVersion;
        }

        public String getSrcApiToken() {
            return srcApiToken;
        }

        public String getSrcName() {
            return srcName;
        }

    }
    private class DarData {
        private String darName;
        private String darUsername;
        private String darPassword;
        private String darUserAffiliation;

        public DarData(String darName, String darUsername, String darPassword, String darUserAffiliation) {
            this.darName = darName;
            this.darUsername = darUsername;
            this.darPassword = darPassword;
            this.darUserAffiliation = darUserAffiliation;
        }

        public String getDarUsername() {
            return darUsername;
        }

        public String getDarPassword() {
            return darPassword;
        }

        public String getDarName() {
            return darName;
        }

        public String getDarUserAffiliation() {
            return darUserAffiliation;
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