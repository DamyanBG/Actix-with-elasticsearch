use std::env;
use std::error::Error;

use actix_web::{get, post, web::{self, Data}, App, HttpResponse, HttpServer, Responder};
use elasticsearch::{auth::Credentials, http::{response::Response, transport::Transport}, Elasticsearch, SearchParts};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

struct Config {
    api_key: String,
    api_key_id: String,
    cloud_id: String,
}

struct ElSearch {
    client: Elasticsearch,
}

impl ElSearch {
    fn new_cloudhost(config: &Config) -> Self {
        let credentials = Credentials::ApiKey(config.api_key_id.to_string(), config.api_key.to_string());
        let transport = Transport::cloud(&config.cloud_id, credentials).unwrap();

        let es_client = Elasticsearch::new(transport);

        ElSearch {
            client: es_client
        }
    }

    async fn add_document(&self, index_name: &str, body: &Value) -> Result<Response, Box<dyn Error>> {
        let response = self.client
            .index(elasticsearch::IndexParts::Index(index_name))
            .body(body)
            .send()
            .await?;
        Ok(response)
    }

    async fn query_all(&self, index_name: &str) -> Result<Response, Box<dyn Error>> {
        let query_body = json!({
            "query": {
                "match_all": {}
            }
        });

        let response = self.client
            .search(SearchParts::Index(&[index_name]))
            .body(query_body)
            .send()
            .await?;

        Ok(response)
    }
}

impl Clone for ElSearch {
    fn clone(&self) -> Self {
        ElSearch {
            client: self.client.clone()
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct Ingredients(Vec<String>);

#[derive(Deserialize, Serialize, Debug)]
struct PizzaCreate {
    name: String,
    description: String,
    price: f32,
    ingredients: Ingredients,
}

#[derive(Deserialize, Serialize, Debug)]
struct Pizza {
    id: String,
    name: String,
    description: String,
    price: f32,
    ingredients: Ingredients,
}

impl Pizza {
    fn new(pizza_data: PizzaCreate, id: String) -> Self {
        Pizza {
            id,
            name: pizza_data.name,
            description: pizza_data.description,
            price: pizza_data.price,
            ingredients: pizza_data.ingredients,
        }
    }
}

#[get("/all-pizzas")]
async fn get_all_pizzas(es: web::Data<ElSearch>) -> impl Responder {
    let response = es.query_all("pizzas_dev").await.unwrap();

    let resp_body = response.json::<Value>().await.unwrap();

    let mut pizzas = Vec::<Pizza>::new();

    for hit in resp_body["hits"]["hits"].as_array().unwrap() {
        let pizza_data: PizzaCreate = serde_json::from_value(hit["_source"].clone()).unwrap();
        let id = serde_json::from_value(hit["_id"].clone()).unwrap();

        let pizza = Pizza::new(pizza_data, id);
        pizzas.push(pizza);
    };

    HttpResponse::Ok().json(pizzas)
}

#[post("/pizza")]
async fn post_pizza(es: web::Data<ElSearch>, pizza_data: web::Json<PizzaCreate>) -> impl Responder {
    let post_body = json!(pizza_data);
    let resp = es.add_document("pizzas_dev", &post_body).await.unwrap();

    if resp.status_code() != 201 {
        return HttpResponse::BadGateway().body("Can not create the pizza!")
    }

    let resp_body = resp.json::<Value>().await.unwrap();
    let id = serde_json::from_value(resp_body["_id"].clone()).unwrap();

    let pizza = Pizza::new(pizza_data.0, id);

    HttpResponse::Ok().json(pizza)
}


#[actix_web::main] // or #[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    // std::env::set_var("RUST_LOG", "debug");
    // env_logger::init();

    let config = Config {
        api_key: env::var("API_KEY").unwrap(),
        api_key_id: env::var("API_KEY_ID").unwrap(),
        cloud_id: env::var("CLOUD_ID").unwrap(),
    };

    let es = ElSearch::new_cloudhost(&config);

    let app_data = Data::new(es);

    HttpServer::new(move || {
        App::new()
            .app_data(app_data.clone())
            .service(get_all_pizzas)
            .service(post_pizza)
    })
    .bind(("127.0.0.1", 8080))?
    .workers(2)
    .run()
    .await
}