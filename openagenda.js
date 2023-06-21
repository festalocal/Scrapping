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

/**==============================================
 **              listEvents
 *? Récupère les événements d'un agenda donné
 *  @param {Object} params Les paramètres à utiliser pour la requête à l'API
 *      - key: la clé d'API
 *      - agendaUid: l'identifiant de l'agenda
 *      - size: le nombre d'événements à récupérer par requête (par défaut 20)
 *      - after: utile pour récupérer les résultats suivants
 *  @return {Object} Les données reçues de l'API
 *=============================================**/
async function listEvents({ key, agendaUid, size = 20, after = '', searchWord } = {}) {
    // Construit l'URL de l'API avec l'UID de l'agenda
    const url = new URL(`https://api.openagenda.com/v2/agendas/${agendaUid}/events`);

    // Prépare les paramètres à utiliser dans la requête
    const params = { key, size, after, searchWord };

    // Supprime les paramètres qui ne sont pas fournis (c'est-à-dire ceux qui sont encore à leur valeur par défaut '')
    Object.keys(params).forEach(key => {
        if (params[key] === '') delete params[key];
    });

    // Ajoute les paramètres à l'URL
    url.search = new URLSearchParams(params).toString();

    //console.log(url)

    // Envoie la requête à l'API
    const response = await fetch(url.toString());

    // Si la requête n'est pas réussie, jette une erreur
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Sinon, extrait les données JSON de la réponse
    const data = await response.json();

    // Renvoie les données
    return data;
}

/**==============================================
 **              main
 *? Cette fonction liste tous les agendas puis 
 *? récupère les événements pour chacun d'eux
 *@param None
 *@return None
 *=============================================**/

async function main() {
    try {
        // Nous définissons un tableau de mots-clés que nous voulons chercher.
        const searchKeywords = ['fête', 'festa', 'feria', 'fete'];

        // Créer un tableau pour stocker tous les agendas
        let allAgendas = [];

        // Nous parcourons chaque mot-clé dans notre tableau.
        for (const keyword of searchKeywords) {
            // Pour chaque mot-clé, nous appelons la fonction "listAgendas" avec ce mot-clé comme paramètre de recherche.
            const agendas = await listAgendas({
                key: '6958c89c91384f01ba90d60be5b1847f', //Clé API
                size: 50, //Nombre d'agendas retournés
                search: keyword, //Mots clés de recherche
                sort: 'createdAt.desc', //Ordre de création décroissant
                official: 1
            });

            // Ajouter les agendas de cette recherche à notre tableau total
            allAgendas = allAgendas.concat(agendas.agendas);
        }

        // Extraire les UIDs des agendas
        const agendaUids = allAgendas.map(agenda => agenda.uid);

        // Parcourt tous les UIDs d'agenda pour récupérer leurs événements
        for (let uid of agendaUids) {
            console.log(`Récupération des événements pour l'agenda ${uid}...`);

            for (const keyword of searchKeywords) {
            // Récupère les 50 premiers événements de l'agenda
            const events = await listEvents({
                key: '6958c89c91384f01ba90d60be5b1847f',  // clé API
                agendaUid: uid,
                size: 50,  // définit la taille de la page à 50,
                search: keyword
            });
            console.log(JSON.stringify(events, null, 2));
            }
            // Affiche les événements pour cet agenda
        }
    } catch (error) {
        // Affiche l'erreur
        console.error('Une erreur est survenue :', error);
    }
}

// Appel de la fonction main
main();
