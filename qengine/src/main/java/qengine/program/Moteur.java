package qengine.program;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

public class Moteur {
	
	static final String baseURI = null;
	static final String workingDir = "data/";
	private String queryFile = workingDir; //sample_query  STAR_ALL_workload
	
	public Moteur(String queryFile) {
		this.queryFile += queryFile;
	}

	private ArrayList<ArrayList<String>> conditionRequete = new ArrayList<>(); // on stocke les patterns des requtes
	private HashMap<Integer,HashSet<String>> resultatRequete = new HashMap<>();
	
	
	public HashMap<Integer, HashSet<String>> getResultatRequete() {
		return resultatRequete;
	}

	public ArrayList<ArrayList<String>> getConditionRequete() {
		return conditionRequete;
	}
	
	public int sizeConditionRequete() {
		return this.getConditionRequete().size();
	}
	
	public void processAQuery(ParsedQuery query) {

			this.conditionRequete.clear();
			
			List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
			for (int i = 0; i < patterns.size(); i++) {
				
				ArrayList<String> listeRessource = new ArrayList<>();
				
				System.out.println("first pattern : " + patterns.get(i));
				//System.out.println("object of the first pattern : " + patterns.get(i).getObjectVar().getValue().toString());
				//System.out.println("predicat of the first pattern : " + patterns.get(i).getPredicateVar().getValue().toString());
				System.out.println("variables to project : ");
				
				listeRessource.add(patterns.get(i).getPredicateVar().getValue().toString());
				listeRessource.add(patterns.get(i).getObjectVar().getValue().toString());
				this.conditionRequete.add((ArrayList<String>) listeRessource.clone());
				listeRessource.clear();
				
				// Utilisation d'une classe anonyme
				query.getTupleExpr().visit(new AbstractQueryModelVisitor<RuntimeException>() {
	
					public void meet(Projection projection) {
						System.out.println(projection.getProjectionElemList().getElements());
					}
				});
			}
		}
	
	public void parseQueries(Index index, Dictionnaire dictionnaire,PrintWriter sortie) throws FileNotFoundException, IOException {

		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();
			
			int numRequete = 1;
			System.out.println("----------------------------------traitement de la requete "+numRequete+"---------------------------------\n");
			while (lineIterator.hasNext())
			
			{
				String line = lineIterator.next();
				queryString.append(line);
				
				if (line.trim().endsWith("}")) {
					
					ParsedQuery query = sparqlParser.parseQuery(queryString.toString(), baseURI);
					sortie.write(queryString.toString()+"\n");
					sortie.write("réponse du systeme: \n");
					
					processAQuery(query);
					
					this.resultatRequete.put(numRequete, intersection(traitement(index,  dictionnaire)));
					
					
					for (String result : this.getResultatRequete().get(numRequete)) {
						sortie.write(result+"\n");
					}
					
					sortie.write("\n");
					queryString.setLength(0); // Reset le buffer de la requéte en chaine vide
					numRequete++;
					System.out.println("----------------------------------FIN methode traitement---------------------------\n");

				}
			}
		}
	}

	public HashMap<Integer,ArrayList<String>> traitement(Index index, Dictionnaire dictionnaire) {
		
		HashMap<Integer,ArrayList<String>> resultatSortie = new HashMap<>();
	
		int cleDictionnaire = this.sizeConditionRequete(); // générons une clé 
		
		System.out.println("recherche des ressources ... \n");
		for (ArrayList<String> arrayList : this.getConditionRequete()) { // on boucle sur les conditions d'une requete
	
			 // l'ordre inverse exemple premier triplet de condition aura la clé la plus grande 
			if (dictionnaire.getDicoInverse().containsKey(arrayList.get(0)) && dictionnaire.getDicoInverse().containsKey(arrayList.get(1))) {
				resultatSortie.put(cleDictionnaire, new ArrayList<>());
				int key = dictionnaire.getCleDicoInverse(arrayList.get(0)); 
				if (index.getIndex().containsKey(key)) {
					int key2 = dictionnaire.getCleDicoInverse(arrayList.get(1));
					if (index.getIndex().get(key).containsKey(key2)){
						
						for (Integer ressource : index.getIndex().get(key).get(key2)) {// on boucle sur la liste au bout de l'indexe pour trouver les ressources
							
							resultatSortie.get(cleDictionnaire).add(dictionnaire.getValeurDico(ressource));
							//System.out.println(dictionnaire.getValeurDico(ressource));		
						}
					}else
						System.out.println("l'index ne contient pas l'élément équivalent à la clé 'O' ");
				}else
					System.out.println("l'index ne contient pas l'élément équivalent à la clé 'P' ");
			cleDictionnaire--;
			}
			else
				System.out.println("ATTENTION :le dictionnaire ne contient pas le prédicat ou l'objet ");
		}
		//System.out.println(resultatSortie);
		return resultatSortie;
	}
	
	public HashSet<String> intersection(HashMap<Integer,ArrayList<String>> cible) {
		HashSet<String> ensembleRessource = new HashSet<>();
		if (!cible.isEmpty()) {
			int key=1;
			
			for (Integer cle : cible.keySet()) {
				key = cle;
				break;
			}
			ensembleRessource.addAll(cible.get(key));
			for (ArrayList<String> arrayList : cible.values()) {
				ensembleRessource.retainAll(arrayList);
			}
			if (!ensembleRessource.isEmpty()) {
				return ensembleRessource;
			}
			else {
				System.out.println("pas de ressource qui satisfait l'ensemble des conditions, la requete n'a pas de résultat !");
				ensembleRessource.add("non disponible");
				return ensembleRessource;
			}
		}
		else {
			System.out.println("vide ");
			ensembleRessource.add("non disponible");
			return ensembleRessource;
		}
	}
}
