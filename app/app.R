library(shiny)
library(pander)

ui <- fluidPage(
  tags$head(
    tags$script(
      type = "module",
      src = "duckdb-wasm.js"
    ),
    tags$script(src = "duckdb-handler.js")
  ),
  textAreaInput(
    inputId = "query",
    label = "Query",
    value = 'select * from "6e6-idx".crashes limit 100'
  ),
  textInput(
    inputId = "name",
    label = "Name"
  ),
  actionButton(
    inputId = "run",
    label = "Run"
  ),
  tableOutput(
    "results"
  ),
  h3("Global Environment:"),
  verbatimTextOutput(
    "environment"
  )
)

server <- function(input, output, session) {
  
  observeEvent(input$run, {
    session$sendCustomMessage("runQuery", list(query = input$query))
  }, ignoreInit = TRUE)
  
  observeEvent(input$duckdb_results, {
    # Parse the results
    result_df <- jsonlite::fromJSON(input$duckdb_results)
    assign(input$name, result_df, envir = .GlobalEnv)
    message(paste("Data assigned to", input$name, "in the global environment"))
  })
  
  output$results <- renderTable({
    req(input$duckdb_results)
    head(jsonlite::fromJSON(input$duckdb_results), 10)
  })
  
  output$environment <- renderPrint({
    ls(envir = .GlobalEnv)
  }) |>
    shiny::bindEvent(
      input$duckdb_results
    )
}

shinyApp(ui = ui, server = server)