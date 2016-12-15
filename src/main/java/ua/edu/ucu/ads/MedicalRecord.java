package ua.edu.ucu.ads;



import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name = "medical_records", schema = "default@hbase_pu")
class MedicalRecord {
	
	@Id
    public byte[] id;

	@Column(name="type")
    public String type;

	@Column(name="description")
    public String description;

	@Column(name="date_performed")
    public Date datePerformed;

	public MedicalRecord() {
	}
	
	public byte[] getId() {
		return id;
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Date getDatePerformed() {
		return datePerformed;
	}

	public void setDatePerformed(Date datePerformed) {
		this.datePerformed = datePerformed;
	}   
}
