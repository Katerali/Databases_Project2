package ua.edu.ucu.ads;



import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name = "physicians", schema = "default@hbase_pu")
public class Physician {

	@Id
    public byte[] id;

	@Column(name="full_name")
    public String fullName;

	@Column(name="clinic_name")
    public String clinicName;

	@Column(name="specialization")
    public String specialization;
	
	public Physician() {
	}

	public byte[] getId() {
		return id;
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public String getClinicName() {
		return clinicName;
	}

	public void setClinicName(String clinicName) {
		this.clinicName = clinicName;
	}

	public String getSpecialization() {
		return specialization;
	}

	public void setSpecialization(String specialization) {
		this.specialization = specialization;
	}
}
