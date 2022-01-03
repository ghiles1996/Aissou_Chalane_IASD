package qengine.program;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

final class Main {
	static final String baseURI = null;
	static String workingDir = "data\\";
	static String fichierDir = "fichier\\100K6000qJVM\\";

	static final String queryFile = "2200q.queryset";// sample_query.queryset STAR_ALL_workload
	static final String mon_fichierM = fichierDir + "sortieMoteur";//
	static final String mon_fichierJ = fichierDir + "sortieJena";//
	static final String mon_fichierFaux = fichierDir + "sortieFausse";
	static final String StatistiqueFichier = fichierDir + "statistique.csv";

	static final String dataFile = workingDir + "100K.nt";// sample_data.nt 100K.nt

	public static long start = 0;
	public static long end = 0;
	public static long startJena = 0;
	public static long endJena = 0;
	public static long startP = 0;
	public static long endP = 0;
	public static long tempsReponseJena = 0;
	public static long tempsParserRequetes = 0;
	public static long tempsCreationDictionnaireIndex = 0;
	public static long tempsDataJena = 0;
	public static long tempsReponseMoteur = 0;
	public static long tempsEchauffementMoteur = 0;
	static boolean JenaVerification = true;
	static boolean warmUp = false;
	static int nombreT = 0;
	static int nombreReq = 0;
	public static Date date = new Date();
	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
	static Jena jena = new Jena();
	public static Model modelJena;

	static int warmPrcnt = 30;

	static HashMap<Integer, HashSet<String>> resultMoteur;
	static HashMap<Integer, HashSet<String>> resultJena;

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws Exception {
		startP = System.currentTimeMillis();
		File directory = new File("");
		String workPath = directory.getAbsolutePath();
		workingDir=workPath+workingDir;
		fichierDir=workPath+fichierDir;
		resultMoteur = new HashMap<>();
		if (JenaVerification) {
			start =System.currentTimeMillis();
			modelJena = jena.createModel(dataFile);
			end =System.currentTimeMillis();
			tempsDataJena = end -start;
		}
// initialisation de dictionnaire/index
		Dictionnaire dictionnaire = new Dictionnaire();
		Index index = new Index(dictionnaire);

		MainRDFHandler mRDFH = new MainRDFHandler(dictionnaire, index);

		System.out.println("\t\t\t----------------------------\t\t\t--------------------------------");
		mRDFH.setApproche("pos");

// création dictionnaire/index
		start = System.currentTimeMillis();
		parseData(mRDFH);
		end = System.currentTimeMillis();
		tempsCreationDictionnaireIndex = end - start;
		System.out.println(
				"temps parse des données et création index et dictionnaire: " + tempsCreationDictionnaireIndex + " ms");
		nombreT=mRDFH.nbrT;
		System.out.println("_____________________________________________________________________________________");

		// **************************************************************************************
		ArrayList<String> requetes = new ArrayList<String>();

		MoteurV2 moteurV2 = new MoteurV2(queryFile);

// parser les requêtes
		start = System.currentTimeMillis();
		requetes = moteurV2.parseQueries();
		// System.out.println(requetes);


// warm  moteur **********************************************************
		if (warmUp) {
			try (FileWriter file = new FileWriter(mon_fichierM, false);
					BufferedWriter buf = new BufferedWriter(file);
					PrintWriter sortie = new PrintWriter(buf)) {
				Random random = new Random();
				int nbrWarm = (warmPrcnt * requetes.size()) / 100;
				int sizeReq = requetes.size();
				ArrayList<String> requeteWarm = new ArrayList<String>();
				while (requeteWarm.size() < nbrWarm) {
					String q = requetes.get(random.nextInt(sizeReq));
					requeteWarm.add(q);
				}
				moteurV2.processAQuery(requeteWarm);

				// echauffement du moteur
				System.out.println("echauffement du moteur avec " + warmPrcnt + "% des requêtes, ce qui fait : "
						+ nbrWarm + " requêtes");
				start = System.currentTimeMillis();
				moteurV2.monMoteur(index, dictionnaire, sortie);
				// for (String q : requeteWarm) {
				// moteurV2.monMoteur(index, dictionnaire, sortie);
				// }

				end = System.currentTimeMillis();
				tempsEchauffementMoteur = end - start;
				System.out.println("Temps d'echauffement:  " + tempsEchauffementMoteur);

				requeteWarm.clear();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
// fin warm  moteur **********************************************************
		
//réponse de notre moteur aux requêtes		
		try (FileWriter file = new FileWriter(mon_fichierM, false);
				BufferedWriter buf = new BufferedWriter(file);
				PrintWriter sortie = new PrintWriter(buf)) {
			moteurV2 = new MoteurV2(queryFile);

			start = System.currentTimeMillis();
			moteurV2.processAQuery(requetes);
			end = System.currentTimeMillis();
			tempsParserRequetes = end - start;
			System.out.println("Temps de parser toutes les requêtes " + tempsParserRequetes + " ms");

			start = System.currentTimeMillis();
			resultMoteur = moteurV2.monMoteur(index, dictionnaire, sortie);
			end = System.currentTimeMillis();
			tempsReponseMoteur = end - start;
			System.out.println("Temps de réponse du moteur:  " + tempsReponseMoteur + " ms");

			// System.out.println("le résultat du traitement est:
			// "+moteurV2.getResultatRequete());

		} catch (IOException e) {
			e.printStackTrace();
		}
		
// réponse de Jena aux requêtes *********************************************************
		if (JenaVerification) {
			try (FileWriter file = new FileWriter(mon_fichierJ, false);
					BufferedWriter buf = new BufferedWriter(file);
					PrintWriter sortie = new PrintWriter(buf)) {
				int cmpt = 1;
				resultJena = new HashMap<>();
				for (String req : requetes) {
					nombreReq++;
					sortie.write("la requete: " + cmpt);
					sortie.write("\nréponse du systeme: \n");
					Query jenaQuery = QueryFactory.create(req);

					startJena = System.currentTimeMillis();
					ResultSet resultSelect = jena.selectJena(jenaQuery, modelJena);
					endJena = System.currentTimeMillis();
					tempsReponseJena += endJena - startJena;

					HashSet<String> oneresult = new HashSet<String>();
					while (resultSelect.hasNext()) {
						String temp = resultSelect.next().toString();
						temp = temp.replaceAll("\\s+", "").replaceAll("\\)\\(", ";").replaceAll("\\(", "")
								.replaceAll("v0=", "").replaceAll("\\)", "").replace("?", "").replace(">", "")
								.replace("<", "").replace("\"", "");
						oneresult.add(temp);
						sortie.write(temp + "\n");
					}
					sortie.write("\n");
					resultJena.put(cmpt, oneresult);
					cmpt++;
				}

				System.out.println("temps de réponde de Jena est: " + tempsReponseJena + " ms");

			} catch (IOException e) {
				e.printStackTrace();
			}
// fin réponse de Jena aux requêtes *********************************************************

			
// verification de l'exactitude des résultats renvoyés par notre moteur avec ceux de Jena
			ArrayList<String> requetesFaux = new ArrayList<String>();
			try (FileWriter file = new FileWriter(mon_fichierFaux, false);
					BufferedWriter buf = new BufferedWriter(file);
					PrintWriter sortie = new PrintWriter(buf)) {

				for (int i = 1; i < requetes.size() + 1; i++) {
					ArrayList<String> resJenaA = new ArrayList<String>(resultJena.get(i));
					ArrayList<String> resMoteurA = new ArrayList<String>(resultMoteur.get(i));
					Collections.sort(resJenaA);
					Collections.sort(resMoteurA);
					HashSet<String> resJena = new HashSet<String>(resJenaA);
					HashSet<String> resMoteur = new HashSet<String>(resJenaA);
					if (!resMoteur.isEmpty() && !resJena.isEmpty()) {
						if (resMoteur.size() == resJena.size()) {
							if (!resMoteur.equals(resJena)) {
								requetesFaux.add(requetes.get(i));
								sortie.write(requetes.get(i).toString() + "\n\n");
								sortie.write("résultat Jena: " + resJena.toString() + "\n\n résulta Moteur: "
										+ resMoteur + "\n\n");
							}
						} else {
							requetesFaux.add(requetes.get(i));
							sortie.write(requetes.get(i).toString() + "different Size \n\n");
							sortie.write("résultat Jena: " + resJena.toString() + "\n\n résulta Moteur: " + resMoteur
									+ "\n\n");
						}
					}
				}
				// System.out.println(requetesFaux.size());
			} catch (IOException e) {
				e.printStackTrace();
			}
			endP = System.currentTimeMillis();
			System.out.println("temps du programme : " +(endP - startP));
			
// export des statistiques
			try (BufferedWriter eval = new BufferedWriter(new FileWriter(StatistiqueFichier, true));) {
				String type = "Moteur";
				if (warmUp) {
					type += "Warm";
				} else {
					type += "Cold";
				}
				if (JenaVerification) {
					type += "WithJena";
				}
				eval.write("\n" + "Date" + ";" + "type de lancement" + ";" + "Input Data" + ";" + "Input Requete" + ";"
						+ "Nombre Triplet" + ";" + "Nombre requete" + ";" + "Temps Data Moteur" + ";" + "Temps Data Jena"+ ";" +"Temps Parse Requêtes"+";"
						+ "Temps reponse Moteur" +";" + "Temps reponse Jena"+";" + "Temps Programme");

				
// sauvegarde
				eval.write("\n" + dateFormat.format(date) + ";" +type +";" + dataFile + ";" + queryFile + ";" + nombreT
						+ ";" + nombreReq + ";" + tempsCreationDictionnaireIndex +";" +tempsDataJena +";" + tempsParserRequetes + ";"
						+ tempsReponseMoteur + ";" + tempsReponseJena + ";" +(endP - startP));

				eval.write("\n");
			}

		}

	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	private static void parseData(MainRDFHandler mRDFH) throws FileNotFoundException, IOException {

		try (Reader dataReader = new FileReader(dataFile)) {
			RDFParser rdfParser = Rio.createParser(RDFFormat.NTRIPLES);
			rdfParser.setRDFHandler(mRDFH);
			rdfParser.parse(dataReader, baseURI);
		}
	}
}
