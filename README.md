# Calcul de la concentration moyenne de radon en suisse

## Que contient ce dépot git?
Ce dépot contient le code servant au calcul de la moyenne de concentration suisse de radon dans les bâtiments suisses. Ce calcul est effectué sur base des données de la base de donnée du radon, administrée par l'Office fédéral de la santé publique (OFSP).
Ce dépot *ne contient pas* les données utilisées pour effectuer le calcul, car ces données ne sont pas publiques.
Ce dépot est rendu public sous licence libre GPLv3, dans le but de permettre sa réutilisation, lors du prochain calcul de la moyenne de radon en suisse par exemple, et dans l'esprit du principe "[Public money? Public code!](https://publiccode.eu/) ".

Pour une description précise des étapes du calcul de la valeur moyenne, lisez le rapport se trouvant sous [report/main.pdf](report/main.pdf). 

## Comment exécuter le code contenu dans ce dépot
### Prérequis
- Les données: la base de données radon, les données pour l'étage d'habitation moyen par canton, les données pour la population par commune
- Les programmes: [sbt >= 1.5.5](https://www.scala-sbt.org/), [scala = 2.12](https://scala-lang.org/), [Open-
Refine >= 3.5.0](https://openrefine.org/) (voir le rapport pour plus de détail quant aux programmes)

### Exécution
- lancer sbt avec `sbt` dans le dossier du projet
- lancer le programme avec `run --v01` ou `run --v02` pour lancer le calcul correspondant (v01: reproduction du calcul de 2004 avec les données mises à jour, v02: approximation des données par une [distribution log-normale](https://fr.wikipedia.org/wiki/Loi_log-normale))
- pour lancer les tests: exécuter la commande `test`

## License
GPLv3, voir [COPYING](COPYING)



## TODO v02 (lognormal fit)
- faire le Q-Q plot par rapport à N(mu, sigma^2)
- faire le Q-Q plot par rapport à d'autres distributions ? directement sur les valeurs et pas le log
  - Pareto?
  - Student t
  - Gamma

## TODO
-   à tester: utiliser max du bâtiment au lieu de la moyenne
-   trouver données fusions de communes pour ne pas exclure mesures de communes disparues
-   moyenne géométrique par maison -> voir si ça fait une différence significative
-   à tester: essayer de prendre seulement les valeurs à l'étage 0 (proposition 2021-11-16)

## DONE
-   [PRI] faire description de toute la chaîne de data processing qui permet de trouver la valeur
    -   Envoyer avec code quand terminé