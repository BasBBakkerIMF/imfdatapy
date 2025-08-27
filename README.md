# imdatapy

## Overview

This guide shows how to use `DataInquiry` from **imfdatapy** to:

- list SDMX dataflows (datasets),
- explore a dataset’s dimensions and codelists,
- build a valid SDMX key with `make_key_str`,
- retrieve time-series data.

### Authentication

- `auth=False`: no auth headers (public datasets like WEO).
- `auth=True`: MSAL authentication (for protected datasets like BBGDL).

## Install

``` python
# If you don't have imfidata yet run the following from a command line
# pip install git+https://github.com/BasBBakkerIMF/imfdatapy.git
```

## Imports

``` python
from imfdatapy import DataInquiry, make_key_str
```

## 1) List datasets (no dataset context needed)

``` python
# Public dataflows (no auth)
all_flows = DataInquiry.datasets(auth=False)
all_flows
```

    AFRREO     Sub-Saharan Africa Regional Economic Outlook (...
    MFS_ODC    Monetary and Financial Statistics (MFS), Other...
    PIP        Portfolio Investment Positions by Counterpart ...
    HPD                             Historical Public Debt (HPD)
    APDREO     Asia and Pacific Regional Economic Outlook (AP...
                                     ...                        
    DIP        Direct Investment Positions by Counterpart Eco...
    SRD                         Structural Reform Database (SRD)
    BOP                                Balance of Payments (BOP)
    QGFS          Quarterly Government Finance Statistics (QGFS)
    PI                                   Production Indexes (PI)
    Length: 68, dtype: object

# Dataflows with auth (if your account grants access to more)

``` python
all_flows = DataInquiry.datasets(auth=True)
all_flows
```

    GPI                                                    Government Policy Indicators (GPI)
    AFRREO                                  Sub-Saharan Africa Regional Economic Outlook (...
    FSI                                                          Financial Stress Index (FSI)
    MFS_ODC                                 Monetary and Financial Statistics (MFS), Other...
    GDS                                                              Global Data Source (GDS)
                                                                  ...                        
    MFS_FMP                                 Monetary and Financial Statistics (MFS): Finan...
    DIP                                     Direct Investment Positions by Counterpart Eco...
    SRD                                                      Structural Reform Database (SRD)
    WEO_LIVE_2020_JAN_ICP2017PPP_VINTAGE           WEO Live 2020 January (ICP2017PPP Weights)
    WTOIMFTT                                                WTO-IMF Tariff Tracker (WTOIMFTT)
    Length: 116, dtype: object

## 2) WEO (public)

Create a client for the `"WEO"` dataset (no auth needed).

``` python
weo = DataInquiry(dataset="WEO", auth=False)
```

### Dimensions and a convenient env

``` python
# Dimensions
dim_df = weo.dimension_names
print(dim_df)
```

         Dimension          Codelist
    0      COUNTRY    CL_WEO_COUNTRY
    1    INDICATOR  CL_WEO_INDICATOR
    2    FREQUENCY           CL_FREQ
    3  TIME_PERIOD              None

``` python
# Build a dot-accessible env mapping Dimension -> Codelist id
dim_env = weo.dimension_env()
```

### Explore codelists (countries, indicators, frequency)

``` python
countries_df, countries  = weo.codelist(dim_env.COUNTRY)
indicators_df, indicators = weo.codelist(dim_env.INDICATOR)
frequency_df, frequency   = weo.codelist(dim_env.FREQUENCY)

countries_df.head(), indicators_df.head(), frequency_df.head()
```

    (  code_id                                               name description
     0   GX123  Other Advanced Economies (Advanced Economies e...            
     1     AFG                   Afghanistan, Islamic Republic of            
     2     ALB                                            Albania            
     3     DZA                                            Algeria            
     4     ASM                                     American Samoa            ,
          code_id                                               name  \
     0        LUR                                  Unemployment rate   
     1    PCOALSA  Coal, South Africa, Export price, US dollars p...   
     2        DSP  External debt: total debt service, amortizatio...   
     3        DSI  External debt: total debt service, interest, U...   
     4  DSP_NGDPD  External debt: total debt service, amortizatio...   
     
                                              description  
     0  The percentage of the labor force that is unem...  
     1  Coal, South African export price, US$ per metr...  
     2  Total external debt amortization charges refle...  
     3  Total external debt interest charges reflect t...  
     4  Total external debt amortization charges refle...  ,
       code_id                   name  \
     0       A                 Annual   
     1       D                  Daily   
     2       M                Monthly   
     3       Q              Quarterly   
     4       S  Half-yearly, semester   
     
                                              description  
     0  To be used for data collected or disseminated ...  
     1  To be used for data collected or disseminated ...  
     2  To be used for data collected or disseminated ...  
     3  To be used for data collected or disseminated ...  
     4  To be used for data collected or disseminated ...  )

### Build a key and fetch data

``` python
key = [
    [countries.United_States, countries.Netherlands_The],
    [indicators.Unemployment_rate],
    [frequency.Annual],
]
keystr = make_key_str(key)
weo_data = weo.get_data(key=keystr,  convert_dates=True)
print(weo_data.tail())
```

        INDICATOR COUNTRY FREQUENCY LATEST_ACTUAL_ANNUAL_DATA OVERLAP SCALE  \
    97        LUR     USA         A                      2024      OL     0   
    98        LUR     USA         A                      2024      OL     0   
    99        LUR     USA         A                      2024      OL     0   
    100       LUR     USA         A                      2024      OL     0   
    101       LUR     USA         A                      2024      OL     0   

                                         METHODOLOGY_NOTES TIME_PERIOD  value  \
    97   Source: National Statistics Office Latest actu...        2026  4.151   
    98   Source: National Statistics Office Latest actu...        2027  4.079   
    99   Source: National Statistics Office Latest actu...        2028  3.929   
    100  Source: National Statistics Office Latest actu...        2029  3.801   
    101  Source: National Statistics Office Latest actu...        2030  3.759   

              date  
    97  2026-12-31  
    98  2027-12-31  
    99  2028-12-31  
    100 2029-12-31  
    101 2030-12-31  

### Codelists summary and a (possibly) protected codelist

``` python
# Summary of all codelists in WEO
codelists = weo.codelists_summary
codelists.tail()
```

<div>

|     | codelist_id        | name                      | version | n_codes |
|-----|--------------------|---------------------------|---------|---------|
| 122 | CL_COMMODITY       | Commodity                 | 2.2.0   | 135     |
| 123 | CL_DERIVATION_TYPE | Derivation Type           | 1.2.1   | 12      |
| 124 | CL_CIVIL_STATUS    | Civil (or Marital) Status | 1.0.1   | 8       |
| 125 | CL_EXRATE          | Exchange Rate             | 1.2.0   | 37      |
| 126 | CL_TOPIC           | Topic                     | 2.2.0   | 118     |

</div>

``` python
cl_df, cl_env = weo.codelist("CL_SEX")
cl_df.head()
```

<div>

|     | code_id | name           | description                                       |
|-----|---------|----------------|---------------------------------------------------|
| 0   | F       | Female         | Female sex assigned at birth.                     |
| 1   | M       | Male           | Male sex assigned at birth.                       |
| 2   | I       | Intersex       | An individual having innate different sex trai... |
| 3   | X       | X              | A category for non-binary individuals, gender ... |
| 4   | \_Z     | Not applicable | Used in response to a question or a request fo... |

</div>

## 3) Bloomberg (protected dataset)

Instantiate with `auth=True` to attach MSAL auth headers.

``` python
bbg = DataInquiry(dataset="BBGDL", auth=True)
bbg_dim_env = bbg.dimension_env()

tickers_df, tickers     = bbg.codelist(bbg_dim_env.TICKER)
fields_df, fields       = bbg.codelist(bbg_dim_env.FIELD)
frequency_df, frequency = bbg.codelist(bbg_dim_env.FREQUENCY)

bbg_key = [
     [tickers.ALBANIAN_LEK_SPOT],
     [fields.Ask_Price, fields.Bid_Price],
     [frequency.Daily],
]
bbg_data = bbg.get_data(key=make_key_str(bbg_key), convert_dates=False)
print(bbg_data.tail())
```

               TICKER   FIELD FREQUENCY SCALE TIME_PERIOD  value
    14288  ALL_CURNCY  PX_BID         D     0  2025-08-19  83.03
    14289  ALL_CURNCY  PX_BID         D     0  2025-08-20  83.02
    14290  ALL_CURNCY  PX_BID         D     0  2025-08-21  83.21
    14291  ALL_CURNCY  PX_BID         D     0  2025-08-22  82.48
    14292  ALL_CURNCY  PX_BID         D     0  2025-08-25  82.68
