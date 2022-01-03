package qengine.program;

import java.util.HashMap;

public class Dictionnaire {
	
	private HashMap<Integer, String> dico;
	private HashMap<String, Integer> dicoInverse;
	private int compteur = 0; 
	
	
	public Dictionnaire() {
		this.dico = new HashMap<>();
		this.dicoInverse = new HashMap<>();
	}
	
	public HashMap<Integer, String> getDico(){
		return this.dico;
	}
	
	public HashMap<String, Integer> getDicoInverse(){
		return this.dicoInverse;
	}
	
	public String getValeurDico(Integer cle) {
		if (getDico().containsKey(cle))
			return getDico().get(cle);
		else
			System.out.println("la clé n'existe pas! ");
			return null;
	}
	
	public Integer getCleDicoInverse(String valeur) {
		if (getDicoInverse().containsKey(valeur))
			return getDicoInverse().get(valeur);
		else
			System.out.println("la ressource n'existe pas! ");
			return null;
	}
		
	public void chargement(String sujet, String predicat, String objet){
			if (!dico.containsValue(sujet) ) {
				this.dico.put(this.compteur, sujet);
				this.dicoInverse.put(sujet,this.compteur);	
				compteur +=1;
			}
			if (!dico.containsValue(predicat) ) {
				this.dico.put(this.compteur, predicat);
				this.dicoInverse.put(predicat,this.compteur);
				compteur +=1;
			}
			if (!dico.containsValue(objet) ) {
				this.dico.put(this.compteur, objet);
				this.dicoInverse.put(objet,this.compteur);
				compteur +=1;
			}
			
		}
	
	
	
	public void affichageDico() {
		for (Integer cle : this.getDico().keySet()) {
			System.out.println("la clé : " +cle+ " à pour ressource : " +dico.get(cle));			
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////////////
	
		
}
