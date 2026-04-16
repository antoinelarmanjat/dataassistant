from typing import TypedDict, List
import pydantic

class ChartDataPoint(TypedDict):
    name: str
    value: float

from google.adk.tools import builtin_tool
@builtin_tool
def create_pie_chart(title: str, data: List[ChartDataPoint]) -> str:
    pass

print("Success")
