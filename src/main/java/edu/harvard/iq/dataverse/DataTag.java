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
@Entity
@Table(indexes = {@Index(columnList="datafile_id")})
public class DataTag extends DataverseEntity implements Serializable {
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
    @Column(columnDefinition = "text", nullable = false)
    private String provenance;

    @NotBlank
    @Column(nullable = false)
    private String tag;

    @NotBlank
    @Column(nullable = false)
    private String colorCode;

    @Column( nullable = false )
    private Timestamp createDate;

    public String getProvenance() {
        return provenance;
    }

    public void setProvenance(String provenance) {
        this.provenance = provenance;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getColorCode() {
        return colorCode;
    }

    public void setColorCode(String colorCode) {
        this.colorCode = colorCode;
    }

    public Timestamp getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Timestamp createDate) {
        this.createDate = createDate;
    }

    /*
     * DataFile to which this tag belongs.
     */
    @OneToOne(cascade={CascadeType.MERGE,CascadeType.PERSIST})
    @JoinColumn(name="datafile_id", unique = true, nullable = false )
    private DataFile dataFile;
    
    public DataFile getDataFile() {
        return this.dataFile;
    }
    
    public void setDataFile(DataFile dataFile) {
        this.dataFile = dataFile;
    }
    

    @Override
    public int hashCode() {
        int hash = 0;
        hash += (id != null ? id.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        // TODO: Warning - this method won't work in the case the id fields are not set
        if (!(object instanceof DataTag)) {
            return false;
        }
        DataTag other = (DataTag) object;
        return !((this.id == null && other.id != null) || (this.id != null && !this.id.equals(other.id)));
    }

    @Override
    public String toString() {
        return "edu.harvard.iq.dataverse.DataTag[ id=" + id + " ]";
    }

}
