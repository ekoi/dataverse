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

    public List<Integer> getListOfDatasetsByStatusAndByDvAlias(String commasparateDvAlias, boolean hasBeenPublisheed) {
        String sql = "select dvo.id from dvobject dvo\n"
                   + "where dvo.identifier is not null\n"
                   + "and dvo.dtype='Dataset' \n";
        if (hasBeenPublisheed)
            sql += "and dvo.publicationdate is not null\n";
        else
            sql += "and dvo.publicationdate is null\n";
        if (!commasparateDvAlias.equals("root")) {
            String commasparateDvIds = convertListIdsToStringCommasparateIds(commasparateDvAlias, "Dataverse");
            sql += "and dvo.owner_id in (" + commasparateDvIds + ")\n";
        }
        sql += "group by dvo.id";
        logger.fine("query - (" + commasparateDvAlias + ") - getListOfDatasetsByStatusAndByDvAlias: " + sql);
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
        logger.fine("query  - (" + dvAlias + ") - getChildrenIdsRecursivelly: " + sql);
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

        logger.fine("query  - (" + dvAlias + ") - getDataversesChildrenRecursively: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDataversesNameByIds(List<Integer> ids) {
        String allIds = ids.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
        logger.fine("allIds: " + allIds);
        if (allIds.isEmpty())
            return Collections.emptyList();
        String sql = "select name, alias, dataversetype\n"
                + "from dataverse\n"
                + "where id in (" + allIds + ") order by name\n";
        logger.fine("query - getDataversesNameByIds: " + sql);
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
        logger.fine("query  - (" + dvAlias + ") - getDataversesNameByStringDate: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDatasetsIdentifierByIds(List<Integer> ids) {
        String allIds = ids.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
        logger.fine("allIds: " + allIds);
        if (allIds.isEmpty())
            return Collections.emptyList();
        String sql = "select (dvo1.authority || '/' || dvo1.identifier) as pid, count(dvo2.owner_id) as num, sum(df.filesize)\n"
                + "from dvobject dvo1\n"
                + "FULL OUTER JOIN dvobject dvo2 on dvo2.owner_id=dvo1.id\n"
                + "FULL OUTER JOIN datafile df on df.id=dvo2.id\n"
                + "where dvo1.id in (" + allIds + ")\n"
                + "group by pid order by num;";
            logger.fine("query - getDatasetsIdentifierByIds: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<String> getDatasetsPIDByStringDate(String dvAlias, String strDate) {
        String sql = "select (dvo.authority || '/' || dvo.identifier) as pid\n"
                + "from dataset ds, dvobject dvo\n"
                + "where dvo.id = ds.id and dvo.dtype='Dataset'\n"
                + "and date_trunc('quarter', dvo.createdate)='" + strDate + "'";
        if (!dvAlias.equals("root")) {
            String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataset");
            if (ids.equals(""))
                return Collections.emptyList();

            sql += "and dvo.id in (" + ids + ")\n";
        }
        logger.fine("query  - (" + dvAlias + ") - getDatasetsPIDByStringDate: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDatasetsByOwnerId(long id) {
        String sql = "select (dvo1.authority || '/' || dvo1.identifier) as pid, count(dvo2.owner_id) as num, sum(df.filesize)\n"
                + "from dvobject dvo1\n"
                + "FULL OUTER JOIN dvobject dvo2 on dvo2.owner_id=dvo1.id\n"
                + "FULL OUTER JOIN datafile df on df.id=dvo2.id\n"
                + "where dvo1.dtype='Dataset'\n"
                + "and dvo2.dtype='DataFile'\n"
                + "and dvo1.owner_id = " + id + "\n"
                + "group by pid order by num;";
        logger.fine("query - getDatasetsByOwnerIds: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> getDatasetsAndDownloadsByOwnerId(long id) {
        String sql = "select (dvo1.authority || '/' || dvo1.identifier) as pid, dvo1.publicationdate, to_char(dvo1.createdate, 'YYYY-MM-DD') as create_date, count(dvo2.owner_id) as num, sum(df.filesize) as size, count(gb.id) as downloads, dvo1.id, dfv.value as title\n"
                + "from dvobject dvo1\n"
                + "FULL OUTER JOIN dvobject dvo2 on dvo2.owner_id=dvo1.id\n"
                + "FULL OUTER JOIN datafile df on df.id=dvo2.id\n"
                + "FULL OUTER JOIN guestbookresponse gb on gb.dataset_id = dvo1.id and df.id = gb.datafile_id\n"
                + "JOIN datasetversion dsv on dsv.dataset_id=dvo1.id\n"
                + "JOIN datasetfield dsf on dsf.datasetversion_id = dsv.id and dsf.datasetversion_id in (select id from datasetversion where dataset_id=dvo1.id order by version DESC FETCH FIRST ROW ONLY)\n"
                + "JOIN datasetfieldvalue dfv on dfv.datasetfield_id = dsf.id\n"
                + "JOIN datasetfieldtype dft  on dft.id = dsf.datasetfieldtype_id and dft.name='title'"
                + "where dvo1.dtype='Dataset'\n"
                + "and dvo1.owner_id !=1\n"
                + "and ( dvo2 is null or dvo2.dtype='DataFile')\n"
                + "and dvo1.owner_id = " + id + "\n"
                + "group by dvo1.id, pid, dvo1.publicationdate, create_date, dfv.value order by num;";
        logger.fine("query - getDatasetsAndDownloadsByOwnerId: " + sql);
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
        logger.fine("query  - (" + dvAlias + ") - dataversesAllTime: " + sql);
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
        logger.fine("query  - (" + dvAlias + ") - dataversesByCategory: " + sql);
        return  em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> dataversesByAlias(String dvAlias) throws Exception {
        String sql = "select dv.id, dv.name, dv.alias, to_char(dvo.createdate, 'YYYY-MM-DD'), dvo.publicationdate, dv.dataversetype\n"
                + "from dataverse dv, dvobject dvo\n"
                + "where dvo.id=dv.id and dvo.dtype='Dataverse'\n";
        if (!dvAlias.equals("root")) {
            String ids = convertListIdsToStringCommasparateIds(dvAlias, "Dataverse");
            if (ids.equals(""))
                return Collections.emptyList();
            sql += "and dv.id in (" + ids + ")\n";
        }
        sql+= "order by dvo.createdate;";
        logger.fine("query  - (" + dvAlias + ") - dataversesByAlias: " + sql);
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
        logger.fine("query  - (" + dvAlias + ") - datasetsAllTime: " + sql);
        return em.createNativeQuery(sql).getResultList();
    }

    public List<Object[]> datasetsBySubject(String dvAlias) {
        dvAlias = dvAlias.replaceAll("\\+", "_");//this is for tilburg_nondsa since the generateMetricName method of edu.harvard.iq.dataverse.Metric

        String sql ="SELECT strvalue, count(dataset.id), dataset.id\n"
                + "FROM datasetfield_controlledvocabularyvalue \n"
                + "JOIN controlledvocabularyvalue ON controlledvocabularyvalue.id = datasetfield_controlledvocabularyvalue.controlledvocabularyvalues_id\n"
                + "JOIN datasetfield ON datasetfield.id = datasetfield_controlledvocabularyvalue.datasetfield_id\n"
                + "JOIN datasetfieldtype ON datasetfieldtype.id = controlledvocabularyvalue.datasetfieldtype_id\n"
                + "JOIN datasetversion ON datasetversion.id = datasetfield.datasetversion_id\n"
                + "JOIN dvobject ON dvobject.id = datasetversion.dataset_id\n"
                + "JOIN dataset ON dataset.id = datasetversion.dataset_id\n"
                + "WHERE\n"
                + "datasetfieldtype.name = 'subject'\n"
                + "and datasetversion.dataset_id in (\n"
                + "select id from dvobject where owner_id in (\n"
                + "WITH RECURSIVE querytree AS (\n"
                + "SELECT id, dtype, owner_id, publicationdate\n"
                + "FROM dvobject\n"
                + "WHERE id in (select id from dataverse where alias in ('" + (dvAlias.replaceAll(",", "','")) + "'))\n"
                + "UNION ALL\n"
                + "SELECT e.id, e.dtype, e.owner_id, e.publicationdate\n"
                + "FROM dvobject e\n"
                + "INNER JOIN querytree qtree ON qtree.id = e.owner_id\n"
                + ")\n"
                + "SELECT id\n"
                + "FROM querytree\n"
                + "where dtype='Dataverse' and owner_id is not null\n"
                + "))\n"
                + "GROUP BY strvalue, dataset.id ORDER BY count(dataset.id) desc;";

        logger.fine("query  - (" + dvAlias + ") - datasetsBySubject: " + sql);
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
        logger.fine("query  - (" + dvAlias + ") - filesAllTime: " + sql);
        Query query = em.createNativeQuery(sql);
        return query.getResultList();
    }

    //TODO: save as cache
    private String convertListIdsToStringCommasparateIds(String dvAlias, String dtype) {
        dvAlias = dvAlias.replaceAll("\\+", "_");//this is for tilburg_nondsa since the generateMetricName method of edu.harvard.iq.dataverse.Metric
        String[] dvIds = Arrays.stream(getChildrenIdsRecursively(dvAlias, dtype, null).stream().mapToInt(i->i).toArray())
                .mapToObj(String::valueOf).toArray(String[]::new);
        return String.join(",", dvIds);
    }

    public String getPath(long id) {
        String sql = "select array_to_string(array(\n"
                + "WITH RECURSIVE rec as\n"
                + "(\n"
                + "SELECT dvobject.id, dvobject.dtype, dvobject.owner_id, 0 as depth from dvobject where id=" + id + "\n"
                + "UNION ALL\n"
                + "SELECT dvobject.id, dvobject.dtype, dvobject.owner_id, depth+ 1 from rec, dvobject where dvobject.id = rec.owner_id\n"
                + ")\n"
                + "SELECT d.name\n"
                + "FROM rec r, dataverse d where d.id=r.id and r.dtype = 'Dataverse' and r.owner_id is not null order by r.depth desc), '-> ');";
        logger.fine("query - getPath: " + sql);
        Query query = em.createNativeQuery(sql);
        return (String)query.getSingleResult();

    }

    public boolean dataverseAliasExist(String dvAlias, boolean topLevelOnly) {
        if (dvAlias == null)
            return false;

        if (dvAlias.trim().isEmpty())
            return false;

        dvAlias = dvAlias.replaceAll("\\+", "_");//this is for tilburg_nondsa since the generateMetricName method of edu.harvard.iq.dataverse.Metric
        int numberOfAliases = dvAlias.split(",").length;
        String sql = "select count(*)= " + numberOfAliases + "\n"
                + "from dataverse dv, dvobject dvo \n"
                + "where dvo.id=dv.id\n";
        if (topLevelOnly)
            sql += "and (dvo.owner_id=1 or dvo.owner_id is null)\n";

        sql += "and dv.alias in ('" + (dvAlias.replaceAll(",", "','")) + "');\n";

        logger.fine("query  - (" + dvAlias + ") - dataverseAliasExist: " + sql);
        Query query = em.createNativeQuery(sql);
        return (boolean)query.getSingleResult();

    }

    /** Downloads */

    public List<Object[]> downloadsAllTime(String commasparateDvAlias) throws Exception {
        commasparateDvAlias = commasparateDvAlias.replaceAll("\\+", "_");//this is for tilburg_nondsa since the generateMetricName method of edu.harvard.iq.dataverse.Metric

        String sql = "select date_trunc('quarter', gb.responsetime)::date as response_time, count(gb.responsetime), sum(df.filesize), (dvo.authority || '/' || dvo.identifier) as pid\n"
                + "from guestbookresponse gb\n"
                + "join datafile df on df.id=gb.datafile_id\n"
                + "join dvobject dvo on dvo.id=gb.dataset_id\n"
                + "where gb.responsetime is not null\n";
        if (!commasparateDvAlias.equals("root")) {
            List<Integer> datasetIds = getListOfDatasetsByStatusAndByDvAlias(commasparateDvAlias, true);
            if (datasetIds.isEmpty())
                return Collections.emptyList();
            String commasparateDatasetIds = datasetIds.stream().map( n -> n.toString() ).collect(Collectors.joining(","));
            sql+= "and dataset_id in (" + commasparateDatasetIds + ")\n";
        }
        sql += "group by response_time, pid order by response_time;";
        logger.fine("query  - (" + commasparateDvAlias + ") - downloadsAllTime: " + sql);
        return  em.createNativeQuery(sql).getResultList();
    }

    public String getTodayAsString() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate today = LocalDate.now();
        return formatter.format(today);
    }
}