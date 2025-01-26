# Local
from .factories.pipeline_factory import PipelineFactory
from .settings import Settings
from .utils.constants import PIPELINES, SCOPE_HELP_TEXT, SUB_SCOPE_HELP_TEXT
import typer
from typing_extensions import Annotated
from typing import Optional

app = typer.Typer()


@app.command()
def run(
    scope:  Annotated[str, typer.Argument(help=SCOPE_HELP_TEXT)], 
    sub_scope:  Annotated[Optional[str], typer.Argument(help=SUB_SCOPE_HELP_TEXT)] = None
    ):
    """Run app.
    Try 'python -m src.run --help' for help.
    """
    pipelines_scope = validate_and_get_scope(scope, sub_scope)
    for pipeline in pipelines_scope:
        settings = Settings(pipeline)
        pipeline_factory = PipelineFactory(settings)
        pipeline = pipeline_factory.create()
        pipeline.run()


def validate_and_get_scope(scope: str, sub_scope: Optional[str]) -> list[str]:
    """Validate scope and sub_scope.
    Return list with pipelines for scope.
    """
    if scope not in ["table", "schema", "all"]:
        raise typer.BadParameter("Scope must be either 'table' or 'schema'.")

    if scope == "table":
        if sub_scope not in PIPELINES.get_all_tables():
            raise typer.BadParameter(f"Table {sub_scope} not found.")
        return [PIPELINES.get_pipeline_from_table(sub_scope)]
    elif scope == "schema":
        if sub_scope not in PIPELINES.schemas:
            raise typer.BadParameter(
                f"Schema {sub_scope} not found. Schema must be one of {PIPELINES.schemas}."
                )
        return PIPELINES.get_layer(sub_scope)
    else:
        return PIPELINES.get_all_pipelines()


if __name__ == "__main__":
    app()
