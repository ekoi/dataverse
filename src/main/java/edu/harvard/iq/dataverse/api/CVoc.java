package edu.harvard.iq.dataverse.api;

import edu.harvard.iq.dataverse.ConceptsCache;
import edu.harvard.iq.dataverse.ConceptsCacheServiceBean;
import edu.harvard.iq.dataverse.util.json.JsonPrinter;


import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.logging.Logger;

import static edu.harvard.iq.dataverse.util.json.JsonPrinter.json;

/**
 * Where the secure, setup API calls live.
 * 
 * @author michael
 */
@Stateless
@Path("cvoc")
public class CVoc extends AbstractApiBean {
	private static final Logger logger = Logger.getLogger(CVoc.class.getName());
	@EJB
	ConceptsCacheServiceBean conceptsCacheServiceBean;

	@GET
	@Path("/conceptsCache")
	public Response getConceptsCache() {
		JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
		for(ConceptsCache conceptsCache : conceptsCacheServiceBean.listAll()) {
			arrayBuilder.add(json(conceptsCache));
		}
		return ok(arrayBuilder);
	}


	@GET
	@Path("/conceptsCache/id/{id}")
	public Response getConceptCacheById(@PathParam("id") long id) {
		ConceptsCache conceptsCache = conceptsCacheServiceBean.getById(id);
		return ok(json(conceptsCache));
	}

	@GET
	@Path("/conceptsCache/concept-uri")
	public Response getConceptCacheByConceptUri(@QueryParam("uri") String concepturi) {
		ConceptsCache conceptsCache = conceptsCacheServiceBean.getByConceptUri(concepturi);
		return ok(json(conceptsCache));
	}
}
