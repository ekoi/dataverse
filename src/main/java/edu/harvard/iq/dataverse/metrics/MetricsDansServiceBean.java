package edu.harvard.iq.dataverse.metrics;

import edu.harvard.iq.dataverse.DatasetVersion;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
                   + "and dvo.dtype='Dataset' \n";
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

    public List<Integer> getChildrenIdsRecursively(String dvAlias, String dtype, DatasetVersion.VersionState versionState) {
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

    public List<Object[]> getDataversesChildrenRecursively(String dvAlias) {
        dvAlias = dvAlias.replaceAll("\\+", "_");//this is for tilburg_nondsa since the generateMetricName method of edu.harvard.iq.dataverse.Metric
        //prohibit the "_" character. The "_" is character reserved for seperator.
        //In the client side, the tilburg_nondsa will convert to tilburg-nondsa
        dvAlias = dvAlias.replaceAll(",", "','");
        String sql =  "WITH RECURSIVE querytree AS (\n"
                + "     SELECT id, dtype, owner_id, publicationdate, 0 as depth\n"
                + "     FROM dvobject\n"
                + "     WHERE id in (select id from dataverse where alias in ('" + dvAlias + "'))\n"
                + "     UNION ALL\n"
                + "     SELECT e.id, e.dtype, e.owner_id, e.publicationdate, depth+ 1\n"
                + "     FROM dvobject e\n"
                + "     INNER JOIN querytree qtree ON qtree.id = e.owner_id\n"
                + ")\n"
                + "SELECT qt.id, depth, dv.alias, dv.name, coalesce(qt.owner_id,0) as ownerId\n"
                + "FROM querytree qt, dataverse dv\n"
                + "where dtype='Dataverse'\n"
                + "and qt.id=dv.id\n"
                + "order by depth asc, ownerId asc;";

        logger.info("query - getDataversesChildrenRecursively: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDataversesNameByIds(List<Integer> ids) {
        String allIds = ids.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
        logger.info("allIds: " + allIds);
        if (allIds.isEmpty())
            return Collections.emptyList();
        String sql = "select name, alias, dataversetype\n"
                + "from dataverse\n"
                + "where id in (" + allIds + ") order by name\n";
        logger.info("query - getDataversesNameByIds: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDataversesNameByStringDate(String strDate, String dvAlias) {
        String sql = "select dv.name, dv.alias, dataversetype\n"
                    + "from dataverse dv, dvobject dvo\n"
                    + "where dvo.id = dv.id and dvo.dtype='Dataverse'\n"
                    + "and date_trunc('year', dvo.createdate)='" + strDate + "'";
        String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataverse");
        if (ids.equals(""))
            return Collections.emptyList();
            sql += "and dvo.id in (" + ids + ");\n";
        logger.info("query - getDataversesNameByStringDate: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDatasetsIdentifierByIds(List<Integer> ids) {
        String allIds = ids.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
        logger.info("allIds: " + allIds);
        if (allIds.isEmpty())
            return Collections.emptyList();
        String sql = "select (dvo1.authority || '/' || dvo1.identifier) as pid, count(dvo2.owner_id) as num, sum(df.filesize)\n"
                + "from dvobject dvo1\n"
                + "FULL OUTER JOIN dvobject dvo2 on dvo2.owner_id=dvo1.id\n"
                + "FULL OUTER JOIN datafile df on df.id=dvo2.id\n"
                + "where dvo1.id in (" + allIds + ")\n"
                + "group by pid order by num;";
            logger.info("query - getDatasetsIdentifierByIds: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<String> getDatasetsPIDByStringDate(String strDate) {
        String sql = "select (dvo.authority || '/' || dvo.identifier) as pid\n"
                + "from dataset ds, dvobject dvo\n"
                + "where dvo.id = ds.id and dvo.dtype='Dataset'\n"
                + "and date_trunc('quarter', dvo.createdate)='" + strDate + "'";
        logger.info("query - getDatasetsPIDByStringDate: " + sql);
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
        String sql = "select date_trunc('quarter', dvo.createdate)::date as create_date, count(dvo.createdate), sum(df.filesize)\n"
                + "from dvobject dvo, datafile df\n"
                + "where dvo.id=df.id and dvo.owner_id in (" + ids + ")\n"
                + "group by create_date order by create_date;";
        logger.info("query - filesAllTime: " + sql);
        Query query = em.createNativeQuery(sql);
        return query.getResultList();
    }

    //TODO: save as cache
    private String convertListIdsToStringCommasparateIds(String dvAlias, String dtype) {
        String[] dvIds = Arrays.stream(getChildrenIdsRecursively(dvAlias, dtype, null).stream().mapToInt(i->i).toArray())
                .mapToObj(String::valueOf).toArray(String[]::new);
        return String.join(",", dvIds);
    }

    /** Downloads */
    public List<Object[]> downloadsAllTimeWithoutPid(String commasparateDvAlias) throws Exception {
        String sql = "select date_trunc('quarter', gb.responsetime)::date as response_time, count(gb.responsetime), sum(df.filesize)\n"
                + "from guestbookresponse gb, datafile df\n"
                + "where responsetime is not null\n"
                + "and gb.datafile_id=df.id\n";
        if (!commasparateDvAlias.equals("root")) {
            List<Integer> datasetIds = getListOfDatasetsByStatusAndByDvAlias(commasparateDvAlias, null);
            if (datasetIds.isEmpty())
                return Collections.emptyList();
            String commasparateDatasetIds = datasetIds.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
            sql+= "and dataset_id in (" + commasparateDatasetIds + ")\n";
        }
        sql += "group by response_time order by response_time;";
        logger.info("query - downloadsAllTime: " + sql);
        return  em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> downloadsAllTime(String commasparateDvAlias) throws Exception {
        String sql = "select date_trunc('quarter', gb.responsetime)::date as response_time, count(gb.responsetime), sum(df.filesize), (dvo.authority || '/' || dvo.identifier) as pid\n"
                + "from guestbookresponse gb\n"
                + "join datafile df on df.id=gb.datafile_id\n"
                + "join dvobject dvo on dvo.id=gb.dataset_id\n"
                + "where gb.responsetime is not null\n";
        if (!commasparateDvAlias.equals("root")) {
            List<Integer> datasetIds = getListOfDatasetsByStatusAndByDvAlias(commasparateDvAlias, null);
            if (datasetIds.isEmpty())
                return Collections.emptyList();
            String commasparateDatasetIds = datasetIds.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
            sql+= "and dataset_id in (" + commasparateDatasetIds + ")\n";
        }
        sql += "group by response_time, pid order by response_time;";
        logger.info("query - downloadsAllTime: " + sql);
        return  em.createNativeQuery(sql).getResultList();
    }

    public String getTodayAsString() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate today = LocalDate.now();
        return formatter.format(today);
    }
}