package ua.edu.ucu.ads;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

public class Testing extends BaseTest {

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

        query = em.createQuery("select p from Patient p where p.firstName = \"Pat\"");
        result = query.getResultList();
        if (result.isEmpty()) {
            System.out.println("FAIL : Patient doesn't exist!");
            exit();
        }
        
        query = em.createQuery("select mr from MedicalRecord mr where mr.type = \"exam\"");
        result = query.getResultList();
        if (!result.isEmpty()) {
            System.out.println("FAIL : MedicalRecord is selected with unexisting type!");
            exit();
        }

        em.close();    
        emf.close();
	}
	
    public static void main(String[] args) {
        Testing test = new Testing();
        try {
			test.runSuite();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}