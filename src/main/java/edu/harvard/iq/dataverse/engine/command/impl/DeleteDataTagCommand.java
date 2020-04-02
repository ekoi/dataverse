package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.engine.command.AbstractCommand;
import edu.harvard.iq.dataverse.engine.command.CommandContext;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.RequiredPermissions;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.engine.command.exception.IllegalCommandException;

import javax.persistence.Query;
import java.util.logging.Logger;


@RequiredPermissions(Permission.EditDataset)
public class DeleteDataTagCommand extends AbstractCommand<DataFile> {

    private static final Logger logger = Logger.getLogger(DeleteDataTagCommand.class.getCanonicalName());

    private DataFile dataFile;

    public DeleteDataTagCommand(DataverseRequest aRequest, DataFile dataFile) {
        super(aRequest, dataFile);
        this.dataFile = dataFile;

    }

    @Override
    public DataFile execute(CommandContext ctxt) throws CommandException {

        try {
            Query query = ctxt.em().createQuery("DELETE from DataTag as dt where dt.dataFile.id =:fileid");
            query.setParameter("fileid", dataFile.getId());
            int i = query.executeUpdate();
            System.out.println(i);
        } catch (Exception ex) {
            String error = "Exception deleting DataTag: " + ex;
            throw new IllegalCommandException(error, this);
        }

        
        return dataFile;
    }

}
