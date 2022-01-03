package qengine.program;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;

public final class MainRDFHandler extends AbstractRDFHandler {

		private Dictionnaire dictionnaire ;
		private Index index;
		String approche;
		static int nbrT=0;
		//private Index index = new Index(dictionnaire);
		
		public String getApproche() {
			return approche;
		}


		public void setApproche(String approche) {
			this.approche = approche;
		}


		public MainRDFHandler(Dictionnaire dictionnaire,Index index) {
			this.dictionnaire = dictionnaire;
			this.index=index;
		}

		@Override
		public void handleStatement(Statement st) {
			nbrT++;
			String approche =this.approche;
			//System.out.println(st.getSubject()+ "\t" + st.getPredicate() +"\t"+st.getObject() );
			dictionnaire.chargement(st.getSubject().toString(), st.getPredicate().toString(), st.getObject().toString());
			index.creationIndex(st.getSubject().toString(), st.getPredicate().toString(), st.getObject().toString(), approche);
		};

}