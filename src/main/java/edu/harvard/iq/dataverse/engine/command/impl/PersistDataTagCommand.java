package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.DataTag;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.engine.command.AbstractCommand;
import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.RequiredPermissions;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.engine.command.exception.IllegalCommandException;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.logging.Logger;


@RequiredPermissions(Permission.EditDataset)
public class PersistDataTagCommand extends AbstractCommand<DataFile> {

    private static final Logger logger = Logger.getLogger(PersistDataTagCommand.class.getCanonicalName());

    private DataFile dataFile;
    private final String jsonInput;
    private final boolean saveContext;
    private final String colorCode;
    private final String tag;

    public PersistDataTagCommand(DataverseRequest aRequest, DataFile dataFile, String colorCode, String tag, String jsonInput, boolean saveContext) {
        super(aRequest, dataFile);
        this.dataFile = dataFile;
        this.jsonInput = jsonInput;
        this.saveContext = saveContext;
        this.colorCode = colorCode;
        this.tag = tag;

    }

    @Override
    public DataFile execute(CommandContext ctxt) throws CommandException {

        try {
            DataTag dt = new DataTag();
            if (dataFile.getDataTag() != null) {
                dt = dataFile.getDataTag();
            }
            dt.setTag(tag);
            dt.setColorCode(colorCode);
            dt.setProvenance(jsonInput);
            dt.setCreateDate(Timestamp.valueOf(LocalDateTime.now()));
            dt.setDataFile(dataFile);
            ctxt.em().merge(dt);
            dataFile.setMergeable(true);
        } catch (Exception ex) {
            String error = "Exception caught persisting DataTag: " + ex;
            throw new IllegalCommandException(error, this);
        }

        if(saveContext) {
            dataFile = ctxt.files().save(dataFile);
        }
        
        return dataFile;
    }

}
