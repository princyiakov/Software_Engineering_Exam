# Software_Engineering_Exam

## We are trying to replicate the  “always fresh” data policy in the view vw_AllSurveyData.

- Here I am trying to replicate the function dbo.fn_GetAllSurveyDataSQL which fetches the entire survey data and dbo.trg_refreshSurveyView which creates a view and keeps it updated on every create, update or insert .

- In Real world scenarios where Data scientists don’t have privileges for creating stored procedures/functions and triggers but only select query option available. 

- This code is created to handle such scenarios.


## Pre-Requisites:
1.Database User should have CREATE/ALTER access
 2.Proper User credentials are updated in the YAML File available in the zip file
 3.The YAML File should be placed in your current working directory
 
