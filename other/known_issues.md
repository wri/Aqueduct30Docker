### Known Issues

The limitations of Aqueduct 3.0 are captured in detail in the technical note. This page contains known issues that we might fix in future versions. This is a live list. 

### Version 3.0

#### Multipolygon islands

For the hydrological basins we used HydroBASINS level 6. A known issue of this input dataset is that island polygons are grouped into a single multipolygon. We have not split these multipolygons into single polygons. Therefore results in small islands need further inspection and please handle with care.

#### Raw values exceeding the range [0-1]

The monthly and annual time series for each indicator are limited to the indicator specific ranges. However, to derive a baseline value, we applied a regression model. This model is not limited to [0-1] and can result in values that do not make sense. 

