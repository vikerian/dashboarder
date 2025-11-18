use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::env;
use std::time::Duration;

// Alias pro typ poolu, abychom nemuseli všude psát `Pool<Postgres>`.
// 'pub' znamená, že tento typ je viditelný i pro mikroslužby, které použijí `common`.
pub type DbPool = Pool<Postgres>;

/// Vytvoří a nakonfiguruje pool připojení k databázi.
/// 
/// Vrací `Result<DbPool, sqlx::Error>`, protože připojení se může nezdařit 
/// (např. špatné heslo, DB neběží).
pub async fn init_db_pool() -> Result<DbPool, sqlx::Error> {
    // 1. NAČTENÍ URL
    // Očekáváme formát: postgres://user:pass@host:port/dbname
    // .expect() způsobí okamžitý pád programu (panic), pokud proměnná chybí.
    // U DB URL je to žádoucí - bez DB nemůžeme fungovat.
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in environment variables");

    tracing::info!("Initializing database connection pool...");

    // 2. KONFIGURACE POOLU (Builder Pattern)
    // PgPoolOptions nám umožňuje nastavit chování poolu před jeho vytvořením.
    let pool = PgPoolOptions::new()
        // Maximální počet otevřených spojení.
        // Pozor: Timescale/Postgres má limit (max_connections).
        // Pokud máš 5 mikroslužeb a každá má max 20, potřebuješ v DB nastavit limit > 100.
        .max_connections(20)
        
        // Minimální počet spojení, která se drží "idle" (připravená).
        // Zrychluje reakci při náhlém nárůstu provozu.
        .min_connections(5)
        
        // Timeout pro získání spojení z poolu.
        // Pokud jsou všechna spojení obsazená déle než 3 sekundy, vyhodí chybu.
        // To je "Fail Fast" princip - lepší vrátit chybu hned, než nechat klienta viset věčně.
        .acquire_timeout(Duration::from_secs(3))
        
        // 3. NAVÁZÁNÍ SPOJENÍ
        // .connect() je asynchronní funkce (.await), protože probíhá síťová komunikace (handshake).
        // Vytvoří pool a líně (lazy) nebo okamžitě naváže spojení.
        .connect(&database_url)
        .await?; // Otazník '?' předá případnou chybu nahoru volajícímu.

    tracing::info!("Database connection pool established.");
    
    // Vrátíme hotový pool (vše je OK, proto zabalíme do Ok())
    Ok(pool)
}