package edu.harvard.iq.dataverse.api;

import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.Metric;
import edu.harvard.iq.dataverse.metrics.MetricsDansServiceBean;
import edu.harvard.iq.dataverse.metrics.MetricsUtil;

import javax.ejb.EJB;
import javax.json.*;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

/**
 * API endpoints for various metrics.
 *
 * These endpoints look a bit heavy because they check for a timely cached value
 * to use before responding. The caching code resides here because the this is
 * the point at which JSON is generated and this JSON was deemed the easiest to
 * cache.
 *
 * @author pdurbin, madunlap
 */
@Path("info/metrics-dans/{topLevelDvAlias}")
public class MetricsDans extends AbstractApiBean {

    private static final Logger logger = Logger.getLogger(MetricsDans.class.getCanonicalName());

    @EJB
    MetricsDansServiceBean metricsSvc;

    /** Dataverses */
    @PathParam("topLevelDvAlias")
    String topLevelDvAlias;
    /** Dataverses */

    @GET
    @Path("dataverses")
    public Response getDataversesAllTime(@Context UriInfo uriInfo) {
        String metricName = createMetricName(topLevelDvAlias,"dataverses");
        try {
            String sanitizedyyyymm = MetricsUtil.sanitizeYearMonthUserInput(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM")));
            String jsonArrayString = metricsSvc.returnUnexpiredCacheMonthly(metricName, sanitizedyyyymm);
            if (null == jsonArrayString) { //run query and save
                List<Integer> releasedDataverse = metricsSvc.getChildrenIdsRecursively(topLevelDvAlias, "Dataverse", DatasetVersion.VersionState.RELEASED);
                logger.info("releasedDataverse: " + releasedDataverse);
                String eko = metricsSvc.getDataversesNameByIds(releasedDataverse).stream().map(n -> n.toString() ).collect(Collectors.joining("|"));
                logger.info("eko: " + eko);
                List<Integer> draftDataverse = metricsSvc.getChildrenIdsRecursively(topLevelDvAlias, "Dataverse", DatasetVersion.VersionState.DRAFT);
                logger.info("draftDataverse: " + draftDataverse);
                String eko2 = metricsSvc.getDataversesNameByIds(draftDataverse).stream().map(n -> n.toString() ).collect(Collectors.joining("|"));
                logger.info("eko2: " + eko2);
                JsonArrayBuilder jsonArrayBuilder = versionStateSizeToJson(releasedDataverse.size(), eko, draftDataverse.size(), eko2, 0, "");
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName, sanitizedyyyymm, jsonArrayString), true); //if not using cache save new
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("dataverses/addedOverTime")
    public Response getDataversesAddedOverTime(@Context UriInfo uriInfo) {
        String metricName = createMetricName(topLevelDvAlias,"dataverses/addedOverTime");
        logger.info(metricName);
        try {
            try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);

                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = dataversesAllYearsToJson(metricsSvc.dataversesAllTime(topLevelDvAlias));
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName, jsonArrayString), false);
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }

        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("dataverses/byCategory")
    public Response getDataversesByCategory() {
        String metricName = createMetricName(topLevelDvAlias,"dataverses/byCategory");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);

            if (null == jsonArrayString) { //run query and save
                JsonArrayBuilder jsonArrayBuilder = MetricsUtil.dataversesByCategoryToJson(metricsSvc.dataversesByCategory(topLevelDvAlias));
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName, jsonArrayString), false);
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    /** Datasets */
    @GET
    @Path("datasets")
    public Response countDatasets(@Context UriInfo uriInfo) {
        String metricName = createMetricName(topLevelDvAlias,"datasets   ");
        try {
            try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);
                if (null == jsonArrayString) { //run query and save
                    List<Integer> publishedDatasets = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, DatasetVersion.VersionState.RELEASED);
                    String eko = metricsSvc.getDatasetsIdentifierByIds(publishedDatasets).stream().map(n -> {Object[] objs = (Object[])n; String s= objs[0].toString() + "-"
                                                        + ((objs[1] != null) ? Long.toString((Long)objs[1]): "0") + "-" + (( objs[2] != null) ? (BigDecimal)objs[2]: "0"); return s;}
                                                    ).collect(Collectors.joining("|"));
                    logger.info("ekoxxxoooxxx: " + eko);
                    List<Integer> draftDatasets = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, DatasetVersion.VersionState.DRAFT);
                    String eko2 = metricsSvc.getDatasetsIdentifierByIds(draftDatasets).stream().map(n -> {Object[] objs = (Object[])n; String s= objs[0].toString() + "-"
                                                        + ((objs[1] != null) ? Long.toString((Long)objs[1]): "0") + "-" + (( objs[2] != null) ? (BigDecimal)objs[2]: "0"); return s;}
                                                    ).collect(Collectors.joining("|"));
                    logger.info("eko2ddddd: " + eko2);
                    List<Integer> deacDatasets = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, DatasetVersion.VersionState.DEACCESSIONED);
                    String eko3 = metricsSvc.getDatasetsIdentifierByIds(deacDatasets).stream().map(n -> {Object[] objs = (Object[])n; String s= objs[0].toString() + "-"
                                                        + ((objs[1] != null) ? Long.toString((Long)objs[1]): "0") + "-" + (( objs[2] != null) ? (BigDecimal)objs[2]: "0"); return s;}
                                                    ).collect(Collectors.joining("|"));
                    logger.info("eko3fffff: " + eko3);
                    JsonArrayBuilder jsonArrayBuilder = versionStateSizeToJson(publishedDatasets.size(), eko, draftDatasets.size(), eko2, deacDatasets.size(), eko3);
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName, jsonArrayString), false);
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("datasets/bySubject")
    public Response getDatasetsBySubject() {
        String metricName = createMetricName(topLevelDvAlias,"datasets/bySubject");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);
            if (null == jsonArrayString) { //run query and save
                JsonArrayBuilder jsonArrayBuilder = MetricsUtil.datasetsBySubjectToJson(metricsSvc.datasetsBySubject(topLevelDvAlias));
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName, jsonArrayString), false);
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("datasets/addedOverTime")
    public Response getDatasetsAddedOverTime(@Context UriInfo uriInfo) {
        String metricName = createMetricName(topLevelDvAlias,"datasets/addedOverTime");
        try {
            try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);
                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = datasetsAllYearsToJson(metricsSvc.datasetsAllTime(topLevelDvAlias));
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName, jsonArrayString), false);
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    /** Files */
    @GET
    @Path("files")
    public Response getFilesAllYears(@Context UriInfo uriInfo) {
        String metricName = createMetricName(topLevelDvAlias,"files");
        try {
            try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);
                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = filesAllYearsToJson(metricsSvc.filesAllTime(topLevelDvAlias), "added");
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName, jsonArrayString), false);
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    /** Files */
    @GET
    @Path("tree")
    public Response getDataversesTree(@Context UriInfo uriInfo) {
        String metricName = createMetricName(topLevelDvAlias,"tree");
        try {
            try {
                String jsonArrayString = null;
                if (null == jsonArrayString) { //run query and save
                    JsonObjectBuilder job = Json.createObjectBuilder();
                    job = dataversesTreeToJson(metricsSvc.getDataversesChildrenRecursively(topLevelDvAlias));
                    jsonArrayString = job.build().toString();
                    metricsSvc.save(new Metric(metricName, jsonArrayString), false);
                }
                return allowCors(ok(MetricsUtil.stringToJsonObjectBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    /** Downloads */
    @GET
    @Path("downloads")
    public Response getDownloadsAllTime() {
        String metricName = createMetricName(topLevelDvAlias,"downloads");
        try {
            try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);
                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = filesAllYearsToJson(metricsSvc.downloadsAllTime2(topLevelDvAlias), "download");
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName, jsonArrayString), false);
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    private JsonObjectBuilder dataversesTreeToJson(List<Object[]> listOfObjectArrays){
        JsonObjectBuilder job = Json.createObjectBuilder();

        if (listOfObjectArrays.isEmpty())
            return job;

        long tempOwnerId=1;
        JsonArrayBuilder childrenArray = Json.createArrayBuilder();
        JsonObjectBuilder jobChild = Json.createObjectBuilder();
        List<Node> nodes = new ArrayList<>();
        for(Object[] objs:listOfObjectArrays) {
            int id = (int)objs[0];
            int depth = (int)objs[1];
            String alias = (String)objs[2];
            String name = (String)objs[3];
            long ownerId = (long)objs[4];
            nodes.add(new Node(alias, String.valueOf(id), String.valueOf(ownerId)));
        }
        String str = createTree(nodes);
        System.out.println("str=========: " + str);
        JsonReader reader = Json.createReader(new StringReader(str));

        JsonArray ja = reader.readArray();

        reader.close();
        return job.add("children", ja);
    }
    private JsonArrayBuilder dataversesAllYearsToJson(List<Object[]> listOfObjectArrays){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        if (listOfObjectArrays.isEmpty())
            return jab;
        int startYear = ((java.sql.Date) listOfObjectArrays.get(0)[0]).toLocalDate().getYear();
        for (Object[] objectArray : listOfObjectArrays) {
            int year = ((java.sql.Date) objectArray[0]).toLocalDate().getYear();
            long count = (long) objectArray[1];
            JsonObjectBuilder job = Json.createObjectBuilder();
            for (startYear++; startYear < year; startYear++) {
                JsonObjectBuilder job2 = Json.createObjectBuilder();
                //fill in the empty years
                job2.add("year", startYear);
                job2.add("created", 0);
                jab.add(job2);
            }
            job.add("year", year);
            job.add("created", count);
            String dvAliases = metricsSvc.getDataversesNameByStringDate(year+"-01-01").stream().map(n -> n.toString() ).collect(Collectors.joining("|"));
            job.add("aliases", dvAliases);
            jab.add(job);
            startYear = year;
        }
        //add empty year of this year
        if (startYear < LocalDate.now().getYear()) {
            startYear++;
            for (; startYear <= LocalDate.now().getYear(); startYear++) {
                JsonObjectBuilder job = Json.createObjectBuilder();
                //fill in the empty years
                job.add("year", startYear);
                job.add("created", 0);
                jab.add(job);
            }
        }
        return jab;
    }

    private JsonArrayBuilder datasetsAllYearsToJson(List<Object[]> listOfObjectArrays){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        if (listOfObjectArrays.isEmpty())
            return jab;
        Map<String, Long> lm= generateQuarterlySequence(((java.sql.Date) listOfObjectArrays.get(0)[0]).toLocalDate(),LocalDate.now());
        for (Object[] objectArray : listOfObjectArrays) {
            LocalDate ld = ((java.sql.Date) objectArray[0]).toLocalDate();
            long i = lm.replace(ld.getYear()+ "Q" + ld.get(IsoFields.QUARTER_OF_YEAR), (long) objectArray[1]);
        }
        lm.forEach((k,v) -> {
            JsonObjectBuilder job = Json.createObjectBuilder();
            job.add("quarter", k);
            job.add("created", v);
            String pids = "";
            if (v.intValue()!=0) {
                String sufficStrDate = "-10-01";
                if (k.endsWith("Q1")){
                    sufficStrDate = "-01-01";
                } else if (k.endsWith("Q2")){
                    sufficStrDate = "-04-01";
                } else if (k.endsWith("Q3")){
                    sufficStrDate = "-07-01";
                }
                String strDate =k.split("Q")[0] + sufficStrDate;
                pids = metricsSvc.getDatasetsPIDByStringDate(strDate).stream().map(n -> n.toString() ).collect(Collectors.joining("|"));
            }
            job.add("pids", pids);
            jab.add(job);
        });
        return jab;
    }

    private JsonArrayBuilder filesAllYearsToJson(List<Object[]> listOfObjectArrays, String type){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        if (listOfObjectArrays.isEmpty())
            return jab;
        String tmpKey = "";
        Map<String, Object[]> lm= generateQuarterlySequence2(((java.sql.Date) listOfObjectArrays.get(0)[0]).toLocalDate(),LocalDate.now());
        for (Object[] objectArray : listOfObjectArrays) {
            LocalDate ld = ((java.sql.Date) objectArray[0]).toLocalDate();

            String key = ld.getYear()+ "Q" + ld.get(IsoFields.QUARTER_OF_YEAR);
            if (tmpKey.equals(key)) {
                Object[] objs = lm.get(key);
                objs[0] = (long)objs[0] + (long) objectArray[1];
                objs[1] = (long)objs[1] + ((BigDecimal) objectArray[2]).longValue();
                String desc = objectArray[3].toString() + "-" + (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                objs[2] = objs[2] + "#" + desc;
            } else {
                String desc = objectArray[3].toString() + "-" + (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                Object[] l = { objectArray[1], ((BigDecimal) objectArray[2]).longValue(), desc};
                Object[] i = lm.replace(key, l);
            }
            tmpKey = key;
        }
        lm.forEach((k,v) -> {
            JsonObjectBuilder job = Json.createObjectBuilder();
            job.add("quarter", k);
            job.add(type, (long)v[0]);
            job.add("size", (long)v[1]);
            job.add("desc", (String)v[2]);
            jab.add(job);
        });
        return jab;
    }

    private JsonArrayBuilder filesAllYearsToJson2(List<Object[]> listOfObjectArrays, String type){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        if (listOfObjectArrays.isEmpty())
            return jab;
        String tmpKey = "";
        Map<String, Object[]> lm= generateQuarterlySequence2(((java.sql.Date) listOfObjectArrays.get(0)[0]).toLocalDate(),LocalDate.now());
        for (Object[] objectArray : listOfObjectArrays) {
            LocalDate ld = ((java.sql.Date) objectArray[0]).toLocalDate();

            String key = ld.getYear()+ "Q" + ld.get(IsoFields.QUARTER_OF_YEAR);
            if (tmpKey.equals(key)) {
                JsonObjectBuilder job = Json.createObjectBuilder();
                Object[] objs = lm.get(key);
                objs[0] = (long)objs[0] + (long) objectArray[1];
                objs[1] = (long)objs[1] + ((BigDecimal) objectArray[2]).longValue();
                String desc = objectArray[3].toString() + "-" + (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                objs[2] = objs[2] + "#" + desc;
            } else {
                String desc = objectArray[3].toString() + "-" + (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                Object[] l = { objectArray[1], ((BigDecimal) objectArray[2]).longValue(), desc};
                Object[] i = lm.replace(key, l);
            }
            tmpKey = key;
        }
        lm.forEach((k,v) -> {
            JsonObjectBuilder job = Json.createObjectBuilder();
            job.add("quarter", k);
            job.add(type, (long)v[0]);
            job.add("size", (long)v[1]);
            JsonArrayBuilder jabDesc = Json.createArrayBuilder();
            Arrays.stream(((String) v[2]).split("#")).forEach(i-> {
                String strs[]=i.split("-");
                if (strs.length == 3) {
                    JsonObjectBuilder jobDesc = Json.createObjectBuilder();
                    jobDesc.add("pid", strs[0]);
                    jobDesc.add("count", strs[1]);
                    jobDesc.add("size", strs[2]);
                    jabDesc.add(jobDesc);
                }
            });
            job.add("desc", jabDesc);
            jab.add(job);
        });
        return jab;
    }

    private Map<String, Long> generateQuarterlySequence(LocalDate startDate, LocalDate endDate) {
        // first truncate startDate to first day of quarter
        int startMonth = startDate.getMonthValue();
        startMonth-= (startMonth - 1) % 3;
        startDate = startDate.withMonth(startMonth).withDayOfMonth(1);
        Map<String, Long> quarterSequence = new LinkedHashMap<>();

        // iterate thorough quarters
        LocalDate currentQuarterStart = startDate;
        while (! currentQuarterStart.isAfter(endDate)) {
            quarterSequence.put(currentQuarterStart.format(DateTimeFormatter.ofPattern("uuuuQQQ", Locale.ENGLISH)), 0L);
            currentQuarterStart = currentQuarterStart.plusMonths(3);
        }
        return quarterSequence;
    }

    private Map<String, Object[]> generateQuarterlySequence2(LocalDate startDate, LocalDate endDate) {
        // first truncate startDate to first day of quarter
        int startMonth = startDate.getMonthValue();
        startMonth-= (startMonth - 1) % 3;
        startDate = startDate.withMonth(startMonth).withDayOfMonth(1);
        Map<String, Object[]> quarterSequence = new LinkedHashMap<>();

        // iterate thorough quarters
        LocalDate currentQuarterStart = startDate;
        while (! currentQuarterStart.isAfter(endDate)) {
            Object[] l = {0L, 0L, ""};
            quarterSequence.put(currentQuarterStart.format(DateTimeFormatter.ofPattern("uuuuQQQ", Locale.ENGLISH)), l);
            currentQuarterStart = currentQuarterStart.plusMonths(3);
        }
        return quarterSequence;
    }

    private String createMetricName(String prefix, String path) {
        logger.info("dvAlias: " + prefix + "\tpath: " + path);
        if (prefix.length() > 30)
            prefix = prefix.substring(0, 30);
        return prefix + "-" + path;
    }

    private JsonArrayBuilder versionStateSizeToJson(int numberOfPublished, String publishedDetails,
                                                    int numberOfDraft, String draftDetails,
                                                    int numberOfDeaccessioned, String deaccessionedDetails){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        JsonObjectBuilder jobPublished = Json.createObjectBuilder();
        jobPublished.add("state", "PUBLISHED");//DatasetVersion.VersionState.RELEASED.name()
        jobPublished.add("count", numberOfPublished);
        jobPublished.add("name", publishedDetails);
        jab.add(jobPublished);
        JsonObjectBuilder jobDraft = Json.createObjectBuilder();
        jobDraft.add("state", DatasetVersion.VersionState.DRAFT.name());
        jobDraft.add("count", numberOfDraft);
        jobDraft.add("name", draftDetails);
        jab.add(jobDraft);
        JsonObjectBuilder jobDeac = Json.createObjectBuilder();
        jobDeac.add("state", DatasetVersion.VersionState.DEACCESSIONED.name());
        jobDeac.add("count", numberOfDeaccessioned);
        jobDeac.add("name", deaccessionedDetails);
        jab.add(jobDeac);
        return jab;
    }

    private static String createTree(List<Node> nodes) {

        Map<String, Node> mapTmp = new HashMap<>();

        //Save all nodes to a map
        for (Node current : nodes) {
            mapTmp.put(current.getId(), current);
        }

        //loop and assign parent/child relationships
        for (Node current : nodes) {
            String parentId = current.getParentId();

            if (parentId != null) {
                Node parent = mapTmp.get(parentId);
                if (parent != null) {
                    current.setParent(parent);
                    parent.addChild(current);
                    mapTmp.put(parentId, parent);
                    mapTmp.put(current.getId(), current);
                }
            }

        }


        //get the root
        Node root = null;
        for (Node node : mapTmp.values()) {
            if(node.getParent() == null) {
                root = node;
                break;
            }
        }

        return root.toString();
    }

    class Node {

        private String id;
        private String parentId;

        private String alias;
        private Node parent;

        private List<Node> children;

        public Node() {
            super();
            this.children = new ArrayList<>();
        }

        public Node(String alias, String childId, String parentId) {
            this.alias = alias;
            this.id = childId;
            this.parentId = parentId;
            this.children = new ArrayList<>();
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getParentId() {
            return parentId;
        }

        public void setParentId(String parentId) {
            this.parentId = parentId;
        }

        public Node getParent() {
            return parent;
        }

        public void setParent(Node parent) {
            this.parent = parent;
        }

        public List<Node> getChildren() {
            return children;
        }

        public void setChildren(List<Node> children) {
            this.children = children;
        }

        public void addChild(Node child) {
            if (!this.children.contains(child) && child != null)
                this.children.add(child);
        }

        public JsonObjectBuilder toString2() {
            JsonObjectBuilder jobPublished = Json.createObjectBuilder();
            jobPublished.add("id", id);
            jobPublished.add("parentId", parentId);
            jobPublished.add("alias", alias);
            JsonArrayBuilder jab = Json.createArrayBuilder();
            for (Node n:children) {
                jab.add(n.toString());
            }
            jobPublished.add("children", jab);

            return jobPublished;
        }
    }


}
