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
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

public class MoteurV2 {

	static final String baseURI = null;
	static final String workingDir = "data/";
	private String queryFile = workingDir; //sample_query  STAR_ALL_workload

	private HashMap<Integer,ArrayList<ArrayList<String>>> conditionRequete = new HashMap<>(); // on stocke les patterns des requtes
	private HashMap<Integer,HashSet<String>> resultatRequete = new HashMap<>();


	public MoteurV2(String queryFile) {
		this.queryFile += queryFile;
	}

	public HashMap<Integer, HashSet<String>> getResultatRequete() {
		return resultatRequete;
	}

	public HashMap<Integer,ArrayList<ArrayList<String>>> getConditionRequete() {
		return conditionRequete;
	}

	public int sizeConditionRequete() {
		return this.getConditionRequete().size();
	}

	public void processAQuery(ArrayList<String> requetes) { //yakhi y'a pas de doublons!

		SPARQLParser sparqlParser = new SPARQLParser();

		int numeroRequete = 1;
		for (String requete : requetes) {
			ParsedQuery query = sparqlParser.parseQuery(requete.toString(), baseURI);
			this.conditionRequete.put(numeroRequete, new ArrayList<ArrayList<String>>());

			List<StatementPattern> patterns = StatementPatternCollector.process(query.getTupleExpr());
			for (int i = 0; i < patterns.size(); i++) {

				ArrayList<String> listeRessource = new ArrayList<>();

				listeRessource.add(patterns.get(i).getPredicateVar().getValue().toString());
				listeRessource.add(patterns.get(i).getObjectVar().getValue().toString());
				
				this.conditionRequete.get(numeroRequete).add((ArrayList<String>) listeRessource.clone());
				listeRessource.clear();
			}
			numeroRequete++;
		}
	}

	public ArrayList<String> parseQueries() throws FileNotFoundException, IOException {
		
		ArrayList<String> requetes = new ArrayList<String>();

		try (Stream<String> lineStream = Files.lines(Paths.get(queryFile))) {
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();

			while (lineIterator.hasNext())

			{
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {

					requetes.add(queryString.toString());

					queryString.setLength(0); 
				}
			}
		}
		return requetes;
	}

	public HashMap<Integer, HashSet<String>>  monMoteur(Index index, Dictionnaire dictionnaire, PrintWriter sortie) {
		System.out.println("----------------------------------debut methode traitement---------------------------\n");

		for (int i = 1; i < this.conditionRequete.size()+1; i++) {

			this.resultatRequete.put(i, intersection(traitement(this.conditionRequete.get(i), index,  dictionnaire)));
			sortie.write("la requete: "+i);
			sortie.write("\nréponse du systeme: \n");
			for (String result : this.getResultatRequete().get(i)) {
				sortie.write(result+"\n");
			}
			
			sortie.write("\n");

		}
		
		System.out.println("----------------------------------FIN methode traitement---------------------------\n");
		return this.resultatRequete;
	}

	public HashMap<Integer,ArrayList<String>> traitement(ArrayList<ArrayList<String>> conditions, Index index, Dictionnaire dictionnaire) {

		HashMap<Integer,ArrayList<String>> resultatSortie = new HashMap<>();

		int cleDictionnaire = conditions.size(); // générons une clé 

		for (ArrayList<String> arrayList : conditions) { // on boucle sur les conditions d'une requete

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
					}else {}
						//System.out.println("l'index ne contient pas l'élément équivalent à la clé 'O' ");
				}else {}
					//System.out.println("l'index ne contient pas l'élément équivalent à la clé 'P' ");
				cleDictionnaire--;
			}else {}
				//System.out.println("ATTENTION :le dictionnaire ne contient pas le prédicat ou l'objet ");
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
//				System.out.println("pas de ressource qui satisfait l'ensemble des conditions, la requete n'a pas de résultat !");
//				ensembleRessource.add("non disponible");
				return ensembleRessource;
			}
		}
		else {
//			System.out.println("vide ");
//			ensembleRessource.add("non disponible");
			return ensembleRessource;
		}
	}
}
