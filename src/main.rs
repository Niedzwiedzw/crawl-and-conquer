use futures::StreamExt;
use std::{
    collections::HashSet,
    sync::Arc,
};
use tokio::sync::RwLock;
use url::Host;

use clap::{
    Parser,
    Subcommand,
};
use eyre::{
    ContextCompat,
    Result,
    WrapErr,
};
use itertools::Itertools;
use tracing::{
    debug,
    error,
    info,
    instrument,
    warn,
};
use tracing_subscriber::{
    fmt::Layer,
    prelude::__tracing_subscriber_SubscriberExt,
    EnvFilter,
};

use scraper::{
    html::Select,
    Html,
    Selector,
};

#[derive(Subcommand)]
enum Commands {
    Crawl,
    // /// does testing things
    // Test {
    //     /// lists test values
    //     #[clap(short, long)]
    //     list: bool,
    // },
}
pub mod prelude {
    pub use tracing::{
        debug,
        error,
        info,
        instrument,
        trace,
        warn,
    };
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(short, long, value_name = "URL", parse(try_from_str))]
    base_address: url::Url,
    // /// Optional name to operate on
    // name: Option<String>,

    // /// Sets a custom config file
    // #[clap(short, long, parse(from_os_str), value_name = "FILE")]
    // config: Option<PathBuf>,

    // /// Turn debugging information on
    // #[clap(short, long, parse(from_occurrences))]
    // debug: usize,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Link {
    pub url: url::Url,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewedLink {
    pub url: url::Url,
    pub source: url::Url,
    pub text: String,
}
fn normalized_url(url: url::Url) -> url::Url {
    let text = url.as_str().trim_end_matches("#");
    url::Url::parse(text).unwrap_or(url)
}
impl ViewedLink {
    pub fn normalized(self) -> Self {
        let Self { url, source, text } = self;
        Self {
            url: normalized_url(url),
            source: normalized_url(source),
            text: text.trim().to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Page {
    pub page_url: url::Url,
}

impl Page {
    pub async fn crawl_url(url: &url::Url, depth_config: CrawlerDepthConfig) -> Result<()> {
        match depth_config.decreased() {
            Some(depth_config) => {
                let page = Page::get(url.clone());
                debug!("crawling {url}");
                page.crawl(depth_config).await?;
            }
            None => {}
        };
        Ok(())
    }
    #[instrument(skip(url), level = "debug")]
    pub fn spawn_crawl(
        url: url::Url,
        depth_config: CrawlerDepthConfig,
    ) -> tokio::task::JoinHandle<()> {
        debug!("spawning crawler for {url}");
        tokio::task::spawn(async move {
            match Self::crawl_url(&url, depth_config).await {
                Ok(_) => {}
                Err(e) => error!("{e:?}"),
            }
        })
    }

    #[instrument(skip_all, fields(viewed_link.url = %viewed_link.url))]
    pub async fn should_crawl_all_links(
        &self,
        depth_config: &CrawlerDepthConfig,
        viewed_link: &ViewedLink,
    ) -> bool {
        if depth_config.remaining_depth == 0 {
            warn!("maximum depth reached");
            return false;
        }
        let desired_host = Some(depth_config.host.to_owned());
        let url_host = viewed_link.url.host().map(|v| v.to_owned());
        if desired_host != url_host {
            warn!(
                "this is a different host :: desired_host={desired_host:?}, url_host={url_host:?}"
            );
            return false;
        }
        if depth_config
            .already_viewed
            .already_viewed_url(&viewed_link.url)
            .await
        {
            // warn!("already viewed");
            return false;
        }
        true
    }

    #[instrument(skip_all)]
    pub async fn crawl(&self, depth_config: CrawlerDepthConfig) -> Result<()> {
        // let tasks = Arc::new(RwLock::new(vec![]));
        let mut tasks = vec![];
        for link in self.links().await?.into_iter().unique() {
            let viewed_link = ViewedLink {
                url: link.url.clone(),
                source: self.page_url.clone(),
                text: link.text,
            }
            .normalized();

            if self
                .should_crawl_all_links(&depth_config, &viewed_link)
                .await
            {
                tasks.push(Self::spawn_crawl(link.url, depth_config.clone()));
            }

            depth_config.already_viewed.view(viewed_link.clone()).await;
        }
        let _: Vec<_> = futures::stream::iter(tasks)
            .buffer_unordered(64)
            .collect()
            .await;
        Ok(())
    }
    #[instrument(skip(self))]
    pub async fn html(&self) -> Result<Html> {
        get_html(&self.page_url).await
    }
    #[instrument(skip_all)]
    pub async fn links(&self) -> Result<Vec<Link>> {
        let current_url = self.page_url.clone();
        let links = self
            .html()
            .await?
            .select(&A_SELECTOR)
            .into_iter()
            .filter_map(|e| -> Option<Link> {
                let raw_url = e.value().attr("href").map(|v| v.to_string())?;

                match url::Url::parse(&raw_url) {
                    Ok(url) => {
                        let url = if url.cannot_be_a_base() {
                            current_url
                                .join(url.as_str())
                                .wrap_err_with(|| format!("combining [{current_url}] with [{url}]"))
                        } else {
                            Ok(url)
                        };
                        match url {
                            Ok(url) => Some(Link {
                                url,
                                text: e.text().collect_vec().join(" "),
                            }),
                            Err(e) => {
                                error!("{e:?}");
                                None
                            }
                        }
                    }
                    Err(error) => {
                        let combined = current_url.join(&raw_url).wrap_err_with(|| {
                            format!("combining [{current_url}] with [{raw_url}]")
                        });

                        match combined {
                            Ok(url) => Some(Link {
                                url,
                                text: e.text().collect_vec().join(" "),
                            }),
                            Err(e) => {
                                error!("{e:?}");
                                None
                            }
                        }
                    }
                }
            })
            .collect_vec();
        info!("found {} links on {}", links.len(), current_url);
        Ok(links)
    }

    #[instrument(fields(page_url = %page_url))]
    pub fn get(page_url: url::Url) -> Self {
        Self { page_url }
    }
}

lazy_static::lazy_static! {
    pub static ref A_SELECTOR: Selector = {
        Selector::parse("a").expect("bad 'a' selector")
    };
}

// impl Page {
//     pub fn links(&self) -> Vec<Link> {
//         self.content.fin
//     }
// }

#[derive(Debug, Clone, Default)]
pub struct AlreadyViewed {
    pub viewed: Arc<RwLock<HashSet<ViewedLink>>>,
    viewed_urls: Arc<RwLock<HashSet<url::Url>>>,
}

impl AlreadyViewed {
    #[instrument(skip_all)]
    pub async fn view(&self, url: ViewedLink) {
        debug!("{:?}", url);
        self.viewed_urls.write().await.insert(url.url.clone());
        self.viewed.write().await.insert(url);
    }
    pub async fn already_viewed(&self, url: &ViewedLink) -> bool {
        self.viewed.read().await.contains(url)
    }

    pub async fn already_viewed_url(&self, url: &url::Url) -> bool {
        self.viewed_urls.read().await.contains(url)
    }
}

#[derive(Debug, Clone)]
pub struct CrawlerDepthConfig {
    pub host: Host,
    pub remaining_depth: usize,
    pub already_viewed: AlreadyViewed,
}

impl CrawlerDepthConfig {
    pub fn decreased(self) -> Option<Self> {
        let Self {
            host: domain,
            remaining_depth,
            already_viewed,
        } = self;
        (remaining_depth > 0).then(move || Self {
            host: domain,
            remaining_depth: remaining_depth - 1,
            already_viewed,
        })
    }
}

#[instrument]
pub async fn get_text(url: &url::Url) -> Result<String> {
    Ok(reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .use_rustls_tls()
        .build()
        .wrap_err("building http client")?
        .get(url.to_string())
        .send()
        .await
        .wrap_err_with(|| format!("scraping [{url}]"))?
        .text()
        .await?)
}
#[instrument]
pub async fn get_html(url: &url::Url) -> Result<scraper::Html> {
    get_text(url).await.map(|text| Html::parse_fragment(&text))
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(Layer::new().with_writer(std::io::stderr));
    tracing::subscriber::set_global_default(subscriber)
        .context("Unable to set a global subscriber")?;
    let Cli {
        base_address,
        command,
    } = Cli::parse();
    match command {
        Commands::Crawl => {
            let already_viewed: AlreadyViewed = Default::default();
            match Page::spawn_crawl(
                base_address.clone(),
                CrawlerDepthConfig {
                    host: base_address
                        .host()
                        .wrap_err("base address must contain a domain")?
                        .to_owned(),
                    remaining_depth: 64,
                    already_viewed: already_viewed.clone(),
                },
            )
            .await
            {
                Ok(_) => {
                    let viewed = already_viewed.viewed.read().await;
                    let unique_links = viewed
                        .iter()
                        .unique()
                        .sorted_unstable_by_key(|v| &v.url)
                        .group_by(|viewed_link| &viewed_link.url)
                        .into_iter()
                        .map(|(url, viewed_link)| {
                            (
                                url,
                                viewed_link
                                    .into_iter()
                                    .map(|ViewedLink { url, source, text }| {
                                        format!("[{source}]:{text}")
                                    })
                                    .collect_vec()
                                    .join("---"),
                            )
                        })
                        .collect_vec();
                    for (url, sources) in unique_links {
                        println!("{url}\t{sources}");
                    }
                }
                Err(e) => error!("{e:#?}"),
            };
        }
    }

    Ok(())
}
