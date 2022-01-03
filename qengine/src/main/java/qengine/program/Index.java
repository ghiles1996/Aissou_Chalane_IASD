package qengine.program;

import java.util.ArrayList;
import java.util.HashMap;

import org.eclipse.rdf4j.model.Value;

public class Index {
	
	private Dictionnaire dictionnaire;
	private HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> index;
		
	
	public Index (Dictionnaire dictionnaire) {
		this.dictionnaire = dictionnaire;
		this.index = new HashMap<>();
		
	}
	
	public HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> getIndex(){
		return this.index;
	}
	
	public int getSizeIndex() {
		return this.getIndex().size();
	}
	
	public ArrayList<String> type(String sujet, String predicat, String objet,String approche){
		//methode pour gerer toute la combinatoire des indexe 
		String ressource1="";
		String ressource2="";
		String ressource3="";
		ArrayList<String> entiers = new ArrayList<>();
		
		if (approche.equals("spo")) {
			ressource1=sujet;
			ressource2=predicat;
			ressource3=objet;
		}
		if (approche.equals("sop")) {
			ressource1=sujet;
			ressource2=objet;
			ressource3=predicat;
		}
		if (approche.equals("pso")) {
			ressource1=predicat;
			ressource2=sujet;
			ressource3=objet;
		}
		if (approche.equals("pos")) {
			ressource1=predicat;
			ressource2=objet;
			ressource3=sujet;
		}
		if (approche.equals("osp")) {
			ressource1=objet;
			ressource2=sujet;
			ressource3=predicat;
		}
		if (approche.equals("ops")) {
			ressource1=objet;
			ressource2=predicat;
			ressource3=sujet;
		}
		entiers.add(ressource1);
		entiers.add(ressource2);
		entiers.add(ressource3);
		return entiers ;
	}
		
	public void creationIndex(String sujet,String predicat, String objet, String approche) {
		
		String ressource1 = this.type(sujet,predicat,objet,approche).get(0);
		String ressource2 =this.type(sujet,predicat,objet,approche).get(1);
		String ressource3 =this.type(sujet,predicat,objet,approche).get(2);
					
		ArrayList<Integer> listeIntermediaire = new ArrayList<>();
		HashMap<Integer,ArrayList<Integer>> hashmap = new HashMap<>();
		
		if (!index.containsKey(dictionnaire.getCleDicoInverse(ressource1))) {
			
			listeIntermediaire.add(dictionnaire.getCleDicoInverse(ressource3));
			hashmap.put(dictionnaire.getCleDicoInverse(ressource2), listeIntermediaire);
			this.index.put(dictionnaire.getCleDicoInverse(ressource1), hashmap);
		}
		else {
			if (!this.index.get(dictionnaire.getCleDicoInverse(ressource1)).containsKey(dictionnaire.getCleDicoInverse(ressource2))) {
				listeIntermediaire.add(dictionnaire.getCleDicoInverse(ressource3));
				this.index.get(dictionnaire.getCleDicoInverse(ressource1)).put(dictionnaire.getCleDicoInverse(ressource2), listeIntermediaire);
			}
			else {
				this.index.get(dictionnaire.getCleDicoInverse(ressource1)).get(dictionnaire.getCleDicoInverse(ressource2)).add(dictionnaire.getCleDicoInverse(ressource3));
			}				
		}
	}
}

