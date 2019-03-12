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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;

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

@Path("info/metrics-dans2/{topLevelDvAlias}")
public class MetricsDans2 extends Metrics {

    private static final Logger logger = Logger.getLogger(MetricsDans2.class.getCanonicalName());

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
                List<Integer> releasedDataverse = metricsSvc.getRecursiveChildrenIds(topLevelDvAlias, "Dataverse", DatasetVersion.VersionState.RELEASED);
                logger.info("releasedDataverse: " + releasedDataverse);
                List<Integer> draftDataverse = metricsSvc.getRecursiveChildrenIds(topLevelDvAlias, "Dataverse", DatasetVersion.VersionState.DRAFT);
                logger.info("draftDataverse: " + draftDataverse);
                JsonArrayBuilder jsonArrayBuilder = versionStateSizeToJson(releasedDataverse.size(), draftDataverse.size(), 0);
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
                    int publishedDataset = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, DatasetVersion.VersionState.RELEASED).size();
                    int draftDataset = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, DatasetVersion.VersionState.DRAFT).size();
                    int deacDataset = metricsSvc.getListOfDatasetsByStatusAndByDvAlias(topLevelDvAlias, DatasetVersion.VersionState.DEACCESSIONED).size();
                    JsonArrayBuilder jsonArrayBuilder = versionStateSizeToJson(publishedDataset, draftDataset, deacDataset);
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
                    JsonArrayBuilder jsonArrayBuilder = datasetsAllYearsToJson(metricsSvc.filesAllTime(topLevelDvAlias));
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

    /** Downloads */
    @GET
    @Path("downloads")
    public Response getDownloadsAllTime() {
        String metricName = createMetricName(topLevelDvAlias,"downloads");
        try {
            try {
                String jsonArrayString = metricsSvc.returnUnexpiredCacheAllTime(metricName);
                if (null == jsonArrayString) { //run query and save
                    JsonArrayBuilder jsonArrayBuilder = datasetsAllYearsToJson(metricsSvc.downloadsAllTime(topLevelDvAlias));
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
    private String createMetricName(String prefix, String path) {
        logger.info("dvAlias: " + prefix + "\tpath: " + path);
        if (prefix.length() > 30)
            prefix = prefix.substring(0, 30);
        return prefix + "-" + path;
    }

    private JsonArrayBuilder versionStateSizeToJson(int numberOfPublished, int numberOfDraft, int numberOfDeaccessioned){
        JsonArrayBuilder jab = Json.createArrayBuilder();
        JsonObjectBuilder jobPublished = Json.createObjectBuilder();
        jobPublished.add("name", "PUBLISHED");//DatasetVersion.VersionState.RELEASED.name()
        jobPublished.add("value", numberOfPublished);
        jab.add(jobPublished);
        JsonObjectBuilder jobDraft = Json.createObjectBuilder();
        jobDraft.add("name", DatasetVersion.VersionState.DRAFT.name());
        jobDraft.add("value", numberOfDraft);
        jab.add(jobDraft);
        JsonObjectBuilder jobDeac = Json.createObjectBuilder();
        jobDeac.add("name", DatasetVersion.VersionState.DEACCESSIONED.name());
        jobDeac.add("value", numberOfDeaccessioned);
        jab.add(jobDeac);
        return jab;
    }

}
