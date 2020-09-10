package edu.harvard.iq.dataverse.api;

import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.Metric;
import edu.harvard.iq.dataverse.metrics.MetricsDansServiceBean;
import edu.harvard.iq.dataverse.metrics.MetricsUtil;

import javax.ejb.EJB;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("info/metrics-dans/{topLevelDvAlias}")
public class MetricsDans extends AbstractApiBean {
    private static final Logger logger = Logger.getLogger(MetricsDans.class.getCanonicalName());

    @EJB
    MetricsDansServiceBean metricsSvc;
    @PathParam("topLevelDvAlias")
    String topLevelDvAlias;

    @GET
    @Path("dataverses")
    public Response getDataversesAllTime(@Context UriInfo uriInfo) {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias,"dataverses");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                List<Integer> releasedDataverse = metricsSvc.getChildrenIdsRecursively(topLevelDvAlias, "Dataverse", DatasetVersion.VersionState.RELEASED);
                String pubDv = metricsSvc.getDataversesNameByIds(releasedDataverse).stream().map(n -> {String s = (String)n[0] + "#" + (String)n[1] + "#" + (String)n[2]; return s;} ).collect(Collectors.joining("|"));
                List<Integer> draftDataverse = metricsSvc.getChildrenIdsRecursively(topLevelDvAlias, "Dataverse", DatasetVersion.VersionState.DRAFT);
                String drafDv = metricsSvc.getDataversesNameByIds(draftDataverse).stream().map(n -> {String s = (String)n[0] + "#" + (String)n[1] + "#" + (String)n[2]; return s;} ).collect(Collectors.joining("|"));
                JsonArrayBuilder jsonArrayBuilder = versionStateSizeToJson(releasedDataverse.size(), pubDv, draftDataverse.size(), drafDv);
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("dataverses/addedOverTime")
    public Response getDataversesAddedOverTime(@Context UriInfo uriInfo) {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias,"dataverses/addedOverTime");
        logger.fine(metricName);
       try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);

                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = dataversesAllYearsToJson(metricsSvc.dataversesAllTime(topLevelDvAlias));
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
            }
    }

    @GET
    @Path("dataverses/byCategory")
    public Response getDataversesByCategory() {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias,"dataverses/byCategory");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonArrayBuilder jsonArrayBuilder = MetricsUtil.dataversesByCategoryToJson(metricsSvc.dataversesByCategory(topLevelDvAlias));
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
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
            if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias, "datasets");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                List<Integer> publishedDatasets = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, true);
                String pubDs = metricsSvc.getDatasetsIdentifierByIds(publishedDatasets).stream().map(n -> {
                            Object[] objs = (Object[]) n;
                            String s = objs[0].toString() + "-"
                                    + ((objs[1] != null) ? Long.toString((Long) objs[1]) : "0") + "-" + ((objs[2] != null) ? (BigDecimal) objs[2] : "0");
                            return s;
                        }
                ).collect(Collectors.joining("|"));
                List<Integer> draftDatasets = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, false);
                String draftDs = metricsSvc.getDatasetsIdentifierByIds(draftDatasets).stream().map(n -> {
                            Object[] objs = (Object[]) n;
                            String s = objs[0].toString() + "-"
                                    + ((objs[1] != null) ? Long.toString((Long) objs[1]) : "0") + "-" + ((objs[2] != null) ? (BigDecimal) objs[2] : "0");
                            return s;
                        }
                ).collect(Collectors.joining("|"));
                JsonArrayBuilder jsonArrayBuilder = versionStateSizeToJson(publishedDatasets.size(), pubDs, draftDatasets.size(), draftDs);
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("datasets/bySubject")
    public Response getDatasetsBySubject() {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias,"datasets/bySubject");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonArrayBuilder jsonArrayBuilder = datasetsBySubjectToJson(metricsSvc.datasetsBySubject(topLevelDvAlias));
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName, null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("datasets/addedOverTime")
    public Response getDatasetsAddedOverTime(@Context UriInfo uriInfo) {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias,"datasets/addedOverTime");
        try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = datasetsAllYearsToJson(metricsSvc.datasetsAllTime(topLevelDvAlias));
                    jsonArrayString = jsonArrayBuilder.build().toString();
                    metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
                }
                return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
            } catch (Exception ex) {
                return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("files")
    public Response getFilesAllYears(@Context UriInfo uriInfo) {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias, "files");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonArrayBuilder jsonArrayBuilder = filesAllYearsToJson(metricsSvc.filesAllTime(topLevelDvAlias), "added");
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName, null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("tree")
    public Response getDataversesTree(@Context UriInfo uriInfo) {
        if (!isDvAliasExist(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias, "tree");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonObjectBuilder job = Json.createObjectBuilder();
                job = dataversesTreeToJson(metricsSvc.getDataversesChildrenRecursively(topLevelDvAlias));
                jsonArrayString = job.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonObjectBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("dataverses-report")
    public Response getDataversesReport(@Context UriInfo uriInfo) {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias, "dataverses-report");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonObjectBuilder job = Json.createObjectBuilder();
                List<Object[]> dvList = metricsSvc.dataversesByAlias(topLevelDvAlias);
                List<DvReport> dvReports = new ArrayList<>();
                dvList.forEach(dv -> {
                    Long id = (Long) dv[0];
                    String status = "Published";
                    if (dv[4] == null)
                        status = "Draft";
                    List<Object[]> dsLists = metricsSvc.getDatasetsByOwnerId(id);
                    long numbOffiles = 0;
                    long totalStorage = 0;
                    for (Object ds[] : dsLists) {
                        numbOffiles += (long) ds[1];
                        totalStorage += +((BigDecimal) ds[2]).longValue();
                    }
                    String path = metricsSvc.getPath(id);
                    DvReport dvReport = new DvReport(id, (String) dv[1], (String) dv[2], path, (String) dv[3], status, (String) dv[5], dsLists.size(), numbOffiles, totalStorage);
                    dvReports.add(dvReport);
                });
                JsonArrayBuilder jsonArrayBuilder = dataversesReportToJson(dvReports);
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("datasets-report")
    public Response getDatasetsReport(@Context UriInfo uriInfo) {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias, "datasets-report");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonObjectBuilder job = Json.createObjectBuilder();
                List<Object[]> dvList = metricsSvc.dataversesByAlias(topLevelDvAlias);
                List<DsReport> dsReports = new ArrayList<>();
                dvList.forEach(dv -> {
                    Long id = (Long) dv[0];
                    List<Object[]> dsList = metricsSvc.getDatasetsAndDownloadsByOwnerId(id);
                    for (Object ds[] : dsList) {
                        String status = "Published";
                        if (ds[1] == null)
                            status = "Draft";
                        int dsId = (int) ds[6];
                        String path = metricsSvc.getPath(dsId);
                        long storagesize = ((ds[4] == null) ? 0:((BigDecimal) ds[4]).longValue());
                        DsReport dsReport = new DsReport((String) dv[1], (String) dv[2], path, (String) ds[0], (String) ds[7], status, (String) ds[2], (long) ds[3], storagesize, (long) ds[5]);
                        dsReports.add(dsReport);
                    }
                });
                JsonArrayBuilder jsonArrayBuilder = datasetsReportToJson(dsReports);
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    @GET
    @Path("downloads")
    public Response getDownloadsAllTime() {
        if (!isTopLevelDvAlias(topLevelDvAlias))
            return allowCors(error(BAD_REQUEST, "Not found"));

        String metricName = createMetricName(topLevelDvAlias, "downloads");
        try {
            String jsonArrayString = metricsSvc.returnUnexpiredCacheDayBased(metricName, metricsSvc.getTodayAsString(), null);
            if (null == jsonArrayString) { //run query and save
                JsonArrayBuilder jsonArrayBuilder = filesAllYearsToJson(metricsSvc.downloadsAllTime(topLevelDvAlias), "download");
                jsonArrayString = jsonArrayBuilder.build().toString();
                metricsSvc.save(new Metric(metricName + "_" + metricsSvc.getTodayAsString(), null, null, jsonArrayString));
            }
            return allowCors(ok(MetricsUtil.stringToJsonArrayBuilder(jsonArrayString)));
        } catch (Exception ex) {
            return allowCors(error(BAD_REQUEST, ex.getLocalizedMessage()));
        }
    }

    private JsonObjectBuilder dataversesTreeToJson(List<Object[]> listOfObjectArrays){
        JsonObjectBuilder job = Json.createObjectBuilder();
        if (listOfObjectArrays.isEmpty())
            return job;

        List<DataverseNode> dataverseNodes = new ArrayList<>();
        for(Object[] objs:listOfObjectArrays) {
            dataverseNodes.add(new DataverseNode((String)objs[2], (int)objs[0], (int) (long)objs[4], (int)objs[1], (String)objs[3]));
        }
        JsonObjectBuilder str = createTree(dataverseNodes);
        return job.add("children", str);
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
            String dvAliases = metricsSvc.getDataversesNameByStringDate(year+"-01-01", topLevelDvAlias).stream().map(n -> {String s = (String)n[0] + "#" + (String)n[1] + "#" + (String)n[2]; return s;} ).collect(Collectors.joining("|"));
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

    private boolean isTopLevelDvAlias(String alias) {
        return metricsSvc.dataverseAliasExist(alias, true);
    }

    private boolean isDvAliasExist(String alias) {
        return metricsSvc.dataverseAliasExist(alias, false);
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
                pids = metricsSvc.getDatasetsPIDByStringDate(topLevelDvAlias, strDate).stream().map(n -> n.toString() ).collect(Collectors.joining("|"));
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
                String desc = (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                if (type.equals("download")) {
                    desc = objectArray[3].toString() + "-" + (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                    objs[2] = objs[2] + "#" + desc;
                } else
                    objs[2] = objs[2] + "#" + desc;
            } else {
                String desc = (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                if (type.equals("download")) {
                    desc = objectArray[3].toString() + "-" + (long) objectArray[1] + "-" + ((BigDecimal) objectArray[2]).longValue();
                }
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

    private Map<String, Long> generateQuarterlySequence(LocalDate startDate, LocalDate endDate) {
        // first truncate startDate to first day of quarter
        int startMonth = startDate.getMonthValue();
        startMonth-= (startMonth - 1) % 3;
        startDate = startDate.withMonth(startMonth).withDayOfMonth(1);
        Map<String, Long> quarterSequence = new LinkedHashMap<>();

        // iterate through quarters
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

        // iterate through quarters
        LocalDate currentQuarterStart = startDate;
        while (! currentQuarterStart.isAfter(endDate)) {
            Object[] l = {0L, 0L, ""};
            quarterSequence.put(currentQuarterStart.format(DateTimeFormatter.ofPattern("uuuuQQQ", Locale.ENGLISH)), l);
            currentQuarterStart = currentQuarterStart.plusMonths(3);
        }
        return quarterSequence;
    }

    private String createMetricName(String prefix, String path) {
        logger.fine("dvAlias: " + prefix + "\tpath: " + path);
        if (prefix.length() > 255)//This is needed since the max length of column only 255
            prefix = prefix.substring(0, 255);
        return prefix + "-" + path;
    }

    private JsonArrayBuilder versionStateSizeToJson(int numberOfPublished, String publishedDetails,
                                                    int numberOfDraft, String draftDetails){
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
        return jab;
    }

    private JsonArrayBuilder dataversesReportToJson(List<DvReport> dvReports){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        dvReports.forEach(dvReport -> {
            JsonObjectBuilder jobPublished = Json.createObjectBuilder();
            jobPublished.add("name", dvReport.name);
            jobPublished.add("alias", dvReport.alias);
            jobPublished.add("path", dvReport.path);
            jobPublished.add("createDate", dvReport.createDate);
            jobPublished.add("status", dvReport.status);
            jobPublished.add("category", dvReport.category);
            jobPublished.add("numberOfDatasets", dvReport.numberOfDatasets);
            jobPublished.add("numberOfFiles", dvReport.numberOfFiles);
            jobPublished.add("totalStorage", dvReport.totalStorage);
            jab.add(jobPublished);
        });

        return jab;
    }

    private JsonArrayBuilder datasetsReportToJson(List<DsReport> dsReports){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        dsReports.forEach(dsReport -> {
            JsonObjectBuilder jobPublished = Json.createObjectBuilder();
            jobPublished.add("dvName", dsReport.dvName);
            jobPublished.add("dvAlias", dsReport.dvAlias);
            jobPublished.add("dvPath", dsReport.dvPath);
            jobPublished.add("pid", dsReport.pid);
            jobPublished.add("dsTitle", dsReport.title);
            jobPublished.add("createDate", dsReport.createDate);
            jobPublished.add("status", dsReport.status);
            jobPublished.add("numberOfFiles", dsReport.numberOfFiles);
            jobPublished.add("storageSize", dsReport.storageSize);
            jobPublished.add("totalDownloads", dsReport.totalDownloads);
            jab.add(jobPublished);
        });

        return jab;
    }

    private JsonArrayBuilder datasetsBySubjectToJson(List<Object[]> listOfObjectArrays) {
        JsonArrayBuilder jab = Json.createArrayBuilder();
        for (Object[] objectArray : listOfObjectArrays) {
            JsonObjectBuilder job = Json.createObjectBuilder();
            String subject = (String) objectArray[0];
            long count = (long) objectArray[1];
            job.add("subject", subject);
            job.add("count", 1);
            jab.add(job);
        }
        return jab;
    }
    private static JsonObjectBuilder createTree(List<DataverseNode> dataverseNodes) {
        Map<Integer, DataverseNode> mapTmp = new HashMap<>();
        //Save all dataverseNodes to a map
        for (DataverseNode current : dataverseNodes) {
            mapTmp.put(current.getId(), current);
        }
        //loop and assign parent/child relationships
        for (DataverseNode current : dataverseNodes) {
            int parentId = current.getParentId();
            if (parentId != 0) {
                DataverseNode parent = mapTmp.get(parentId);
                if (parent != null) {
                    current.setParent(parent);
                    parent.addChild(current);
                    mapTmp.put(parentId, parent);
                    mapTmp.put(current.getId(), current);
                }
            }
        }
        //get the root
        DataverseNode root = null;
        for (DataverseNode node : mapTmp.values()) {
            if(node.getParent() == null) {
                root = node;
                break;
            }
        }
        return root.build();
    }

    class DataverseNode {
        private int id;
        private int parentId;
        private int depth;
        private String name;
        private String alias;
        private DataverseNode parent;
        private List<DataverseNode> children;
        public DataverseNode() {
            super();
            this.children = new ArrayList<>();
        }

        public DataverseNode(String alias, int childId, int parentId, int depth, String name) {
            this.alias = alias;
            this.id = childId;
            this.parentId = parentId;
            this.children = new ArrayList<>();
            this.depth = depth;
            this.name = name;
        }

        public String getAlias() {
            return alias;
        }

        public void setAlias(String alias) {
            this.alias = alias;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getParentId() {
            return parentId;
        }

        public void setParentId(int parentId) {
            this.parentId = parentId;
        }

        public DataverseNode getParent() {
            return parent;
        }

        public void setParent(DataverseNode parent) {
            this.parent = parent;
        }

        public List<DataverseNode> getChildren() {
            return children;
        }

        public void setChildren(List<DataverseNode> children) {
            this.children = children;
        }

        public void addChild(DataverseNode child) {
            if (!this.children.contains(child) && child != null)
                this.children.add(child);
        }

        public int getDepth() {
            return depth;
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public JsonObjectBuilder build() {
            JsonObjectBuilder jobPublished = Json.createObjectBuilder();
            jobPublished.add("id", id);
            jobPublished.add("parentId", parentId);
            jobPublished.add("alias", alias);
            jobPublished.add("depth", depth);
            jobPublished.add("name", name);
            JsonArrayBuilder jab = Json.createArrayBuilder();
            for (DataverseNode n:children) {
                jab.add(n.build());
            }
            jobPublished.add("children", jab);
            return jobPublished;
        }
    }

    class DvReport {
        private long id;
        private String name;
        private String alias;
        private String path;
        private String createDate;
        private String status;
        private String category;
        private int numberOfDatasets;
        private long numberOfFiles;
        private long totalStorage;

        public DvReport(long id, String name, String alias, String path, String createDate, String status, String category, int numberOfDatasets, long numberOfFiles, long totalStorage) {
            this.id = id;
            this.name = name;
            this.alias = alias;
            this.path = path;
            this.createDate = createDate;
            this.status = status;
            this.category = category;
            this.numberOfDatasets = numberOfDatasets;
            this.numberOfFiles = numberOfFiles;
            this.totalStorage = totalStorage;
        }

    }

    class DsReport {
        private String dvName;
        private String dvAlias;
        private String dvPath;
        private String pid;
        private String title;
        private String status;
        private String createDate;
        private long numberOfFiles;
        private long storageSize;
        private long totalDownloads;

        public DsReport(String dvName, String dvAlias, String dvPath, String pid, String title, String status, String createDate,  long numberOfFiles, long storageSize, long totalDownloads) {
            this.dvName = dvName;
            this.dvAlias = dvAlias;
            this.dvPath = dvPath;
            this.pid = pid;
            this.title = title;
            this.status = status;
            this.createDate = createDate;
            this.numberOfFiles = numberOfFiles;
            this.storageSize = storageSize;
            this.totalDownloads = totalDownloads;
        }
    }

}
