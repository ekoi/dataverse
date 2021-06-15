/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.harvard.iq.dataverse;

import org.hibernate.validator.constraints.NotBlank;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 *
 * @author Eko Indarto
 */
@NamedQueries({
        @NamedQuery( name="ConceptsCache.findAll",
                query="SELECT c FROM ConceptsCache c"),
        @NamedQuery( name="ConceptsCache.findById",
                query = "SELECT c FROM ConceptsCache c WHERE c.id=:id"),
//        @NamedQuery( name="ConceptsCache.findByDatasetVersionId",
//                query = "SELECT c FROM ConceptsCache c WHERE c.datasetVersionId=:datasetVersionId"),
        @NamedQuery( name="ConceptsCache.findByConceptUri",
                query = "SELECT c FROM ConceptsCache c WHERE c.concepturi=:concepturi")
})
@Entity
public class ConceptsCache extends DataverseEntity implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    @NotBlank
    @Column(nullable = false)
    private String concepturi;

    @NotBlank
    @Column(columnDefinition = "text", nullable = false)
    private String conceptjson;

    @Column( nullable = false )
    private Timestamp createdDate;

    public String getConcepturi() {
        return concepturi;
    }

    public void setConcepturi(String concepturi) {
        this.concepturi = concepturi;
    }

    public String getConceptjson() {
        return conceptjson;
    }

    public void setConceptjson(String conceptjson) {
        this.conceptjson = conceptjson;
    }

    public Timestamp getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Timestamp createdDate) {
        this.createdDate = createdDate;
    }

    @ManyToOne(cascade={CascadeType.MERGE,CascadeType.PERSIST})
    @JoinColumn(name="datasetversion_id", nullable = false )
    private DatasetVersion datasetVersion;

    public DatasetVersion getDatasetVersion() {
        return this.datasetVersion;
    }

    public void setDatasetVersion(DatasetVersion datasetVersion) {
        this.datasetVersion = datasetVersion;
    }

//    public Long getDatasetVersionId() {
//        return datasetVersionId;
//    }
//
//    public void setDatasetVersionId(Long datasetVersionId) {
//        this.datasetVersionId = datasetVersionId;
//    }
//
//    private Long datasetVersionId;
    @Override
    public int hashCode() {
        int hash = 0;
        hash += (id != null ? id.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof ConceptsCache)) {
            return false;
        }
        ConceptsCache other = (ConceptsCache) object;
        return !((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id)));
    }

    @Override
    public String toString() {
        return "edu.harvard.iq.dataverse.ConceptsCache[ id=" + id + " ]";
    }

}
