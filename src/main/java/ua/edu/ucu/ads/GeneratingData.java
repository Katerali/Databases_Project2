package ua.edu.ucu.ads;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.lang.ArrayUtils;
import org.fluttercode.datafactory.impl.DataFactory;



public class GeneratingData extends BaseTest {

    private String patientFirstName = null;

    private String mrType = null;

    public static void main(String[] args)
    {
        GeneratingData gd = new GeneratingData();
        gd.generateData();
        try {
            gd.runSuite();
        } catch (IOException ioe) {
        }
    }

    public void generateData() {
		List<String> specializations = new ArrayList<String>();
		specializations.add("otolaryngologist");
		specializations.add("dentist");
		specializations.add("neurologist");
		
		List<String> mrTypes = new ArrayList<String>();
		mrTypes.add("visit");
		mrTypes.add("exam");
		mrTypes.add("prescription");
		mrTypes.add("other");
		

        Map<String, String> props = new HashMap();
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu", props);
        EntityManager em = emf.createEntityManager();

        DataFactory df = new DataFactory();

    	for (int ph = 0; ph < 4; ph++) {
    		Physician physician = new Physician();
    		String phFirstName = df.getFirstName();
    		String phLastName = df.getLastName();
    		String phFullName = phFirstName.concat(" ").concat(phLastName);
    		UUID physicianUUID = UUID.randomUUID();
    		physician.setId(Util.toBytes(physicianUUID));
    		physician.setFullName(phFullName);
    		physician.setClinicName(df.getRandomWord());
    		physician.setSpecialization(df.getItem(specializations));
    		for (int p = 0; p < 4; p++) {
    			Patient patient = new Patient();
    			String pFirstName = df.getFirstName();
                if (this.patientFirstName == null) {
                    this.patientFirstName = pFirstName;
                }
    			String pLastName = df.getLastName();
    			UUID patientUUID = UUID.randomUUID();
    			byte[] patientID = Util.toBytes(patientUUID);
    			patient.setId(patientID);
    			patient.setFirstName(pFirstName);
    			patient.setLastName(pLastName);
    			patient.setDateOfBirth(df.getBirthDate());
            	for (int mr = 0; mr < 10; mr++) {
                    MedicalRecord medicalRecord = new MedicalRecord();
                    byte[] medicalRecordId = ArrayUtils.addAll(patientID, Util.toBytes(UUID.randomUUID()));
                    medicalRecord.setId(medicalRecordId);
                    medicalRecord.setDatePerformed(df.getBirthDate());
                    medicalRecord.setDescription(df.getRandomText(100));
                    String mrType = df.getItem(mrTypes);
                    if (this.mrType == null) {
                        this.mrType = mrType;
                    }
                    medicalRecord.setType(mrType);
                    em.persist(medicalRecord);
            	}
            	em.persist(patient);
    		}
    		em.persist(physician);
    	}

        em.close();
        emf.close();
    }

	@Override
	public void customTest() throws IOException {
		
		Query query;
		List<String> result;

        Map<String, String> props = new HashMap<>();
        // props.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu", props);
        EntityManager em = emf.createEntityManager();
        
        query = em.createQuery("select p from Physician p");
        result = query.getResultList();
        if (result.isEmpty()) {
            System.out.println("FAIL : Physican table is empty!");
            exit();
        }

        query = em.createQuery("select p from Patient p where p.firstName = \"" + this.patientFirstName + "\"");
        result = query.getResultList();
        if (result.isEmpty()) {
            System.out.println("FAIL : Patient doesn't exist!");
            exit();
        }
        
        query = em.createQuery("select mr from MedicalRecord mr where mr.type = \"" + this.mrType + "\"");
        result = query.getResultList();
        if (result.isEmpty()) {
            System.out.println("FAIL : MedicalRecord is selected with unexisting type!");
            exit();
        }

        em.close();    
        emf.close();
	}
}
