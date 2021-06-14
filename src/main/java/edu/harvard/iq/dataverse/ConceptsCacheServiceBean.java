package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.actionlogging.ActionLogServiceBean;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

/**
 * @author Eko Indarto
 */
@Stateless
@Named
public class ConceptsCacheServiceBean {

    @PersistenceContext
    EntityManager em;

    @EJB
    ActionLogServiceBean actionLogSvc;

    public List<ConceptsCache> listAll() {
        return em.createNamedQuery("ConceptsCache.findAll", ConceptsCache.class).getResultList();
    }

    public ConceptsCache getById(long id){
        List<ConceptsCache> cVocCaches = em.createNamedQuery("ConceptsCache.findById", ConceptsCache.class)
                .setParameter("id", id )
                .getResultList();
        return cVocCaches.get(0);
    }
//
//    public ConceptsCache getByDatasetVersionId(long id){
//        List<ConceptsCache> cVocCaches = em.createNamedQuery("ConceptsCache.findByDatasetVersionId", ConceptsCache.class)
//                .setParameter("id", id )
//                .getResultList();
//        return cVocCaches.get(0);
//    }

    public ConceptsCache getByConceptUri(String conceptUri) {
        List<ConceptsCache> conceptsCache = em.createNamedQuery("ConceptsCache.findByConceptUri", ConceptsCache.class)
                .setParameter("concepturi", conceptUri )
                .getResultList();
        if (conceptsCache == null || conceptsCache.isEmpty())
            return null;
        return conceptsCache.get(0);
    }


}