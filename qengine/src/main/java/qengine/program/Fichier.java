package qengine.program;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.apache.commons.cli.*;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;

final class Fichier {
// creation de jena
	static Jena jena = new Jena();
	public static Model modelJena;


	public static int nbrT = 0;
	public static int nbrReq = 0;

// les chemins

	static final String baseURI = null;
	static final String workingDir = "data\\";
	static final String fichierDir = "fichier\\";
	static final String RequeteRep = workingDir + "10MnonDouble";
	static final String queryFile = "sample_query.queryset";// sample_query.queryset STAR_ALL_workload
	static final String mon_fichierM = fichierDir + "sortieMoteur";//
	static final String mon_fichierJ = fichierDir + "sortieJena";//
	static final String mon_fichierFaux = fichierDir + "sortieFausse";
	static final String StatistiqueFichier = fichierDir + "statistique.csv";

	static ArrayList<String> requetes = new ArrayList<String>();
	static int doubles = 0;

	static final String dataFile = workingDir + "300K.nt";// sample_data.nt 100K.nt
	static Dictionnaire dictionnaire;
	static Index index;
	static HashMap<Integer, Integer> NbrRep_req;

//==========================================================================================================
// entree du programme
	public static void main(String[] args) throws Exception {

// Initialisation de dossier de travail
		File[] files = new File(RequeteRep).listFiles();

		modelJena = jena.createModel(dataFile);

//Parsing les requêtes
		System.out.println("Réponse au requêtes:");
		System.out.println("_____________________________________________________________________________________");
		for (File f : files) {
			if (!f.isDirectory()) {
				parseQueries(RequeteRep + "\\" + f.getName().toString());
			}
		}

// Réponse aux requêtes avec Jena
		try (BufferedWriter sortie = new BufferedWriter(
				new FileWriter(fichierDir+ "NouvReq.queryset", false));) {
			int cmptreqVide=0;
			NbrRep_req = new HashMap<Integer, Integer>();
			System.out.println("Réponse aux requêtes par Jena");
			for (String req : requetes) {
				Query jenaQuery = QueryFactory.create(req);
				// reponse par Jena
				ResultSet resultSelect = jena.selectJena(jenaQuery, modelJena);
				// ArrayList<String> resultJena = new ArrayList<String>();
				// Traitement du resultat Jena
				int nbrReponse = 0;
				while (resultSelect.hasNext()) {
					resultSelect.next();
					nbrReponse++;
				}
				// compter le nombre de requete par reponse
				if (NbrRep_req.containsKey(nbrReponse)) {
					int val = NbrRep_req.get(nbrReponse);
					NbrRep_req.put(nbrReponse, val + 1);
				} else {
					NbrRep_req.put(nbrReponse, 1);
				}
				if (nbrReponse == 0) {
					if (cmptreqVide < 346) {
						sortie.write(req + "\n");
						cmptreqVide++;
					}
				} else {
					sortie.write(req + "\n");
				}
			}
		}
		
		try (BufferedWriter sortie = new BufferedWriter(new FileWriter(fichierDir + "Stat300K.csv", true));) {
			String ligne1 = "Requetes;Doubles";
			String ligne2 = nbrReq+";"+doubles;
			for(Map.Entry stat: NbrRep_req.entrySet()) {
				ligne1+=";req_"+stat.getKey();
				ligne2+=";"+stat.getValue();
			}
			sortie.write(ligne1+"\n"+ligne2);
		}
		System.out.println("nombre de requêtes doublé: " + doubles);
		for (Integer cle : NbrRep_req.keySet()) {
			System.out.println("le nombre de requête avec réponse " + cle + " : " + NbrRep_req.get(cle));
		}

	}

	public static void parseQueries(String f) throws FileNotFoundException, IOException {

		try (Stream<String> lineStream = Files.lines(Paths.get(f))) {
			// SPARQLParser sparqlParser = new SPARQLParser();
			Iterator<String> lineIterator = lineStream.iterator();
			StringBuilder queryString = new StringBuilder();

			while (lineIterator.hasNext())

			{
				String line = lineIterator.next();
				queryString.append(line);

				if (line.trim().endsWith("}")) {
					//System.out.println(queryString.toString());
					if (requetes.contains(queryString.toString())) {
						doubles++;
					} else {
						requetes.add(queryString.toString());
					}
					queryString.setLength(0);
				}
			}
			nbrReq++;
		}
	}

}
