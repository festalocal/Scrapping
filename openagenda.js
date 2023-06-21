/**==============================================
 **              listAgendas
 *? Cette fonction fait une requête à l'API OpenAgenda pour récupérer une liste des agendas.
 *? Les paramètres de la requête (comme la clé de l'API, la taille de la page, le mot-clé de recherche, etc.) peuvent être personnalisés.
 *@param {Object} param Un objet contenant les paramètres de la requête
 *@return {Promise<Object>} Une promesse qui résout en un objet contenant les données de la réponse de l'API
 *=============================================**/

async function listAgendas({ key, size = 100, after = '', search = '', official = '', slug = '', uid = '', network = '', sort = '' } = {}) {

    // Nous définissons l'URL de base de l'API que nous allons appeler.
    const url = new URL('https://api.openagenda.com/v2/agendas');

    // Nous créons un objet "params" avec tous les paramètres que nous voulons envoyer avec notre requête API.
    const params = { key, size, after, search, official, slug, uid, network, sort };

    // Nous utilisons une boucle pour parcourir chaque clé dans l'objet "params".
    Object.keys(params).forEach(key => {
        // Si la valeur de la clé est une chaîne vide, nous supprimons cette clé de l'objet.
        if (params[key] === '') delete params[key];
    });

    // Nous ajoutons les paramètres à notre URL. "URLSearchParams" transforme notre objet "params" en une chaîne de caractères utilisable dans une URL.
    url.search = new URLSearchParams(params).toString();

    // Nous envoyons une requête HTTP GET à l'URL que nous avons construite. Nous attendons que la requête soit terminée et stockons la réponse dans "response".
    const response = await fetch(url.toString());

    // Si la réponse n'est pas OK (c'est-à-dire que le statut HTTP n'est pas dans la plage 200-299), nous lançons une erreur.
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Nous convertissons la réponse en JSON (un format facile à utiliser en JavaScript) et nous stockons le résultat dans "data".
    const data = await response.json();

    // Nous retournons les données que nous avons obtenues.
    return data;
}


// Nous définissons un tableau de mots-clés que nous voulons chercher.
const searchKeywords = ['fête', 'festa', 'feria'];

// Nous parcourons chaque mot-clé dans notre tableau.
searchKeywords.forEach(keyword => {
    // Pour chaque mot-clé, nous appelons la fonction "listAgendas" avec ce mot-clé comme paramètre de recherche.
    listAgendas({
        key: '6958c89c91384f01ba90d60be5b1847f', //Clé API
        size: 10, //Nombre d'agendas retournés
        search: keyword, //Mots clés de recherche
        sort: 'createdAt.desc' //Ordre de création décroissant
    })
    // Une fois que la promesse retournée par "listAgendas" est résolue, nous imprimons les résultats dans la console.
    .then(data => {
        console.log(`Résultats pour "${keyword}":`, data);
    })
    // Si une erreur se produit pendant l'exécution de "listAgendas", nous l'attrapons ici et l'imprimons dans la console.
    .catch(error => {
        console.error(`Erreur lors de la recherche pour "${keyword}":`, error);
    });
});
