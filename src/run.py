# Standard library
from typing import Optional

# Third party
import typer
from typing_extensions import Annotated

# Local
from .factories.pipeline_factory import PipelineFactory
from .settings import Settings
from .utils.constants import PIPELINES, SCOPE_HELP_TEXT, SUB_SCOPE_HELP_TEXT, SKIP_HELP_TEXT

app = typer.Typer()


@app.command()
def run(
    scope: Annotated[str, typer.Argument(help=SCOPE_HELP_TEXT)],
    sub_scope: Annotated[Optional[str], typer.Option(help=SUB_SCOPE_HELP_TEXT)] = None,
    skip: Annotated[Optional[str], typer.Option(help=SKIP_HELP_TEXT)] = ""
):
    """Run app.
    Try 'python -m src.run --help' for help.
    """

    pipelines_scope = validate_and_get_scope(scope, sub_scope)
    filtered_pipeline_scope = filter_skip(pipelines_scope, skip)
    for pipeline_name in filtered_pipeline_scope:
        settings = Settings(pipeline_name)
        pipeline_factory = PipelineFactory(settings)
        pipeline = pipeline_factory.create()
        pipeline.run()


def validate_and_get_scope(scope: str, sub_scope: Optional[str]) -> tuple:
    """Validate scope and sub_scope.
    Return list with pipelines for scope.
    """
    if scope not in ["table", "schema", "all"]:
        raise typer.BadParameter("Scope must be either 'table' or 'schema'.")

    if scope == "table":
        if sub_scope not in PIPELINES.get_all_tables():
            raise typer.BadParameter(f"Table {sub_scope} not found.")
        return (PIPELINES.get_pipeline_from_table(sub_scope),)
    elif scope == "schema":
        if sub_scope not in PIPELINES.schemas:
            raise typer.BadParameter(
                f"Schema {sub_scope} not found. Schema must be one of {PIPELINES.schemas}."
            )
        return PIPELINES.get_layer(sub_scope)
    else:
        return PIPELINES.get_all_pipelines()


def filter_skip(pipelines: tuple, skip: str) -> tuple:
    """Select pipelines to skip from table names in --skip."""
    
    if not skip:
        return pipelines
    else:
        tables_to_skip = skip.split(',')
        pipelines_to_skip = []
        for table_name in tables_to_skip:
            if table_name.lower() not in PIPELINES.get_all_tables():
                raise typer.BadParameter(
                    f"Error trying to skip table {table_name}: not found."
                    )
            pipelines_to_skip.append(
                PIPELINES.get_pipeline_from_table(table_name.lower())
                )
        return tuple([p for p in pipelines if p not in pipelines_to_skip])


if __name__ == "__main__":
    app()
