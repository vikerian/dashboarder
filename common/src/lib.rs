// 'pub mod' říká: "Tento soubor existuje a jeho obsah je veřejně přístupný 
// komukoliv, kdo použije knihovnu `common`."
pub mod database;
pub mod logging;
pub mod models;

// Pokud bychom napsali jen 'mod database;', soubor by se načetl, 
// ale byl by soukromý (viditelný jen uvnitř `common`, ne pro `tcp-ingress`).