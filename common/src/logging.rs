use tracing_subscriber::{EnvFilter};

/// Inicializuje logování pro danou mikroslužbu.
///
/// # Argumenty
/// * `service_name` - Název služby (např. "tcp-ingress"), který se objeví v logu.
pub fn init_logging(service_name: &str) {
    // 1. NASTAVENÍ FILTRU (Co se má logovat?)
    // EnvFilter čte proměnnou prostředí `RUST_LOG`.
    // Příklad: RUST_LOG=debug (všechno), RUST_LOG=error (jen chyby).
    // .try_from_default_env() zkusí načíst ENV.
    // .unwrap_or_else(...) říká: "Když proměnná neexistuje, použij default 'info'".
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // 2. NASTAVENÍ PŘÍJEMCE (Kam a jak se má logovat?)
    // `fmt::Subscriber` je to, co formátuje události a posílá je na stdout.
    tracing_subscriber::fmt()
        // Aplikujeme filtr definovaný výše
        .with_env_filter(filter)
        
        // -- FORMÁTOVÁNÍ PRO STROJE (JSON) --
        // Pro IoT a Docker je lepší JSON. Syslog/ELK stack si to pak snadno naparsuje.
        // Výstup bude vypadat: {"level":"INFO","fields":{"message":"..."},"target":"..."}
        .json() 
        
        // Pokud bys chtěl čitelné logy pro člověka při vývoji lokálně, 
        // můžeš .json() smazat a nechat default (pretty print).
        
        // -- KONTEXT --
        // Zahrne ID vlákna (užitečné, ale v async Rustu spíše zajímavost, 
        // protože tasky skáčou mezi vlákny).
        .with_thread_ids(true)
        
        // Target je cesta k modulu (např. tcp_ingress::handler). 
        // Někdy je to moc ukecané, tak to lze vypnout.
        .with_target(true) 
        
        // .init() nastaví tento subscriber jako GLOBÁLNÍ pro celou aplikaci.
        // To se smí stát jen jednou za běh programu.
        .init();
    
    // První logovací zpráva - ověření, že to funguje.
    tracing::info!("Service '{}' logging initialized successfully.", service_name);
}