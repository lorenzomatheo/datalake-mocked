module "scrape_sara" {
  source          = "./pipeline"
  name            = "scrape_sara"
  description     = "Scraper de produtos do site Sara"
  schedule        = "0 0 3 ? * 1" # Every Sunday at 3:00 AM
  notify          = true
  timeout_seconds = 21600 # 6 horas 
  parameters      = { "stage" = "prod", "environment" = "production" }
  tasks = [
    {
      task_key = "scrape_sara"
      path     = "0_any_2_raw/sara/scraper_sara"
      base_parameters = {
        max_products = "10000"
        full_refresh = "false"
      }
    },
    {
      task_key   = "extrai_texto_bulas_sara"
      path       = "1_raw_2_standard/sara/extrai_texto_bulas_sara"
      depends_on = ["scrape_sara"]
    }
  ]
}
