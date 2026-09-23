class PipelineStageError(Exception):
    """Raised when a pipeline stage fails for a system/technical reason.

    Carries the stage name so failures are traceable to exactly where
    they happened, distinct from expected data-quality problems (which
    are handled via quarantine, not exceptions).
    """

    def __init__(self, stage: str, original: Exception):
        self.stage = stage
        self.original = original
        super().__init__(f"[{stage}] {type(original).__name__}: {original}")