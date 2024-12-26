# Local
from .validate import Validator
from ..abstract.step import Step
from ..settings import Settings


class ManyValidator(Step):
    """Validate multiple files.
    Validation based in:
       * Current pipeline settings
       * previous_output["files_path"]
    """

    def __init__(self, previous_output: dict, settings: Settings):
        """Init validator."""
        super(ManyValidator, self).__init__(__name__, previous_output, settings)

        self.files_path: str = self.previous_output["files_path"]
        self.pipeline: str = self.settings.pipeline
        self.pipeline_tables = self.settings.PIPELINE_TABLE

    def run(self):
        """Run validation step for many tables."""

        for table in self.pipeline_tables:
            context = {"file_path": self.previous_output["files_path"][table["name"]]}
            validator = Validator(context, self.settings, table)
            _, output = validator.run()
            self.output.update({table["name"]: output})
        return True, self.output
