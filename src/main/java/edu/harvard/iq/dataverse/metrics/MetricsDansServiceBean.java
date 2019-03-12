package edu.harvard.iq.dataverse.metrics;

import edu.harvard.iq.dataverse.DatasetVersion;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Stateless
public class MetricsDansServiceBean extends MetricsServiceBean implements Serializable {
    private static final Logger logger = Logger.getLogger(MetricsDansServiceBean.class.getCanonicalName());

    @PersistenceContext(unitName = "VDCNet-ejbPU")
    private EntityManager em;

    public List<Integer> getListOfDatasetsByStatusAndByDvAlias(String commasparateDvAlias, DatasetVersion.VersionState versionState) {
        String sql = "select dvo.id from dvobject dvo, datasetversion dsv\n"
                   + "where dvo.id=dsv.dataset_id and dvo.identifier is not null\n"
                   + "and dvo.authority='10411' \n";
        if (versionState != null)
            sql += "and dsv.versionstate = '" + versionState.name() + "'\n";
        if (!commasparateDvAlias.equals("root")) {
            String commasparateDvIds = convertListIdsToStringCommasparateIds(commasparateDvAlias, "Dataverse");
            sql += "and dvo.owner_id in (" + commasparateDvIds + ")\n";
        }
        sql += "group by dvo.id";
        logger.info("getListOfDatasetsByStatusAndByDvAlias query: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Integer> getRecursiveChildrenIds(String dvAlias, String dtype, DatasetVersion.VersionState versionState) {
        dvAlias = dvAlias.replaceAll("\\+", "_");//this is for tilburg_nondsa since the generateMetricName method of edu.harvard.iq.dataverse.Metric
        //prohibit the "_" character. The "_" is character reserved for seperator.
        //In the client side, the tilburg_nondsa will convert to tilburg-nondsa
        dvAlias = dvAlias.replaceAll(",", "','");
        String sql =  "WITH RECURSIVE querytree AS (\n"
                + "     SELECT id, dtype, owner_id, publicationdate\n"
                + "     FROM dvobject\n"
                + "     WHERE id in (select id from dataverse where alias in ('" + dvAlias + "'))\n"
                + "     UNION ALL\n"
                + "     SELECT e.id, e.dtype, e.owner_id, e.publicationdate\n"
                + "     FROM dvobject e\n"
                + "     INNER JOIN querytree qtree ON qtree.id = e.owner_id\n"
                + ")\n"
                + "SELECT id\n"
                + "FROM querytree\n"
                + "where dtype='" + dtype + "' and owner_id is not null\n";
        if (versionState != null ) {
            switch (versionState) {
                case RELEASED: sql += "and publicationdate is not null\n";
                    break;
                case DRAFT:sql += "and publicationdate is null\n";
                    break;
            }
        }
        logger.info("query - getChildrenIdsRecursivelly: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    /** Dataverses */
    public List<Object[]> dataversesAllTime(String dvAlias) throws Exception {
        String sql = "select date_trunc('year', createdate)::date as create_date, count(createdate)\n"
                + "from dataverse\n"
                + "join dvobject on dvobject.id = dataverse.id\n";
        if (!dvAlias.equals("root")) {
            String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataverse");
            if (ids.equals(""))
                return Collections.emptyList();
            sql += "where dvobject.id in (" + ids + ")\n";
        }
        sql += "group by create_date order by create_date;";
        logger.info("query: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> dataversesByCategory(String dvAlias) throws Exception {
        String sql = "select dataversetype, count(dataversetype) from dataverse\n"
                + "join dvobject on dvobject.id = dataverse.id\n";
        if (!dvAlias.equals("root")) {
            String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataverse");
            if (ids.equals(""))
                return Collections.emptyList();
            sql += "where dvobject.id in (" + ids + ")\n";
        }
        sql+= "group by dataversetype order by count desc;";
        logger.info("query: " + sql);
        return  em.createNativeQuery(sql).getResultList();
    }
    
    /** Datasets */
    public List<Object[]> datasetsAllTime(String dvAlias) throws Exception {
        String sql = "select date_trunc('quarter', createdate)::date as create_date, count(createdate)\n"
                + "from dataset\n"
                + "join dvobject on dvobject.id = dataset.id\n";
        if (!dvAlias.equals("root")) {
            String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataset");
            if (ids.equals(""))
                return Collections.emptyList();
            sql += "where dvobject.id in (" + ids + ")\n";
        }
        sql +="group by create_date order by create_date;";
        logger.info("query: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> datasetsBySubject(String dvAlias) {
        String sql ="SELECT strvalue, count(dataset.id)\n"
                + "FROM datasetfield_controlledvocabularyvalue \n"
                + "JOIN controlledvocabularyvalue ON controlledvocabularyvalue.id = datasetfield_controlledvocabularyvalue.controlledvocabularyvalues_id\n"
                + "JOIN datasetfield ON datasetfield.id = datasetfield_controlledvocabularyvalue.datasetfield_id\n"
                + "JOIN datasetfieldtype ON datasetfieldtype.id = controlledvocabularyvalue.datasetfieldtype_id\n"
                + "JOIN datasetversion ON datasetversion.id = datasetfield.datasetversion_id\n"
                + "JOIN dvobject ON dvobject.id = datasetversion.dataset_id\n"
                + "JOIN dataset ON dataset.id = datasetversion.dataset_id\n"
                + "WHERE\n"
                + "datasetfieldtype.name = 'subject'\n";
        if (!dvAlias.equals("root")) {
            String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataset");
            if (ids.equals(""))
                return Collections.emptyList();
            sql += "and datasetversion.dataset_id in (" + ids + ")\n";
        }
        sql += "GROUP BY strvalue ORDER BY count(dataset.id) desc;";
        logger.info("query: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    /** Files */
    public List<Object[]> filesAllTime(String dvAlias) throws Exception {
        String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataset");
        if (ids.equals(""))
            return Collections.emptyList();
        String sql = "select date_trunc('quarter', createdate)::date as create_date, count(createdate)\n"
                + "from dvobject\n"
                + "where owner_id in (" + ids + ")\n"
                + "group by create_date order by create_date;";
        logger.info("query: " + sql);
        Query query = em.createNativeQuery(sql);
        return query.getResultList();
    }

    //TODO: save as cache
    private String convertListIdsToStringCommasparateIds(String dvAlias, String dtype) {
        String[] dvIds = Arrays.stream(getRecursiveChildrenIds(dvAlias, dtype, null).stream().mapToInt(i->i).toArray())
                .mapToObj(String::valueOf).toArray(String[]::new);
        return String.join(",", dvIds);
    }

    /** Downloads */
    public List<Object[]> downloadsAllTime(String commasparateDvAlias) throws Exception {
        String sql = "select date_trunc('quarter', responsetime)::date as response_time, count(responsetime)\n"
                + "from guestbookresponse\n"
                + "where responsetime is not null\n";
        if (!commasparateDvAlias.equals("root")) {
            List<Integer> datasetIds = getListOfDatasetsByStatusAndByDvAlias(commasparateDvAlias, null);
            if (datasetIds.isEmpty())
                return Collections.emptyList();
            String commasparateDatasetIds = datasetIds.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
            sql+= "and dataset_id in (" + commasparateDatasetIds + ")\n";
        }
        sql += "group by response_time order by response_time;";
        logger.info("query: " + sql);
        return  em.createNativeQuery(sql).getResultList();
    }
}