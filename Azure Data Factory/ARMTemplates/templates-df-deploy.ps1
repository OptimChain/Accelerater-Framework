#Connect-AzAccount
#Get-AzSubscription
#Get-AzSubscription -SubscriptionName Visual Studio Professional Subscription 


$Template_Location = 'C:\Users\Jason.Bian\Desktop\Solution\Azure Data Factory\ARMTemplates\arm_template\arm_template.json'

$Template_Parameters = 'C:\Users\Jason.Bian\Desktop\Solution\Azure Data Factory\ARMTemplates\arm_template\arm_template_parameters.json'

New-AzResourceGroupDeployment -Name MyARMDeployment -ResourceGroupName da-dev-wus2-analytics-rg -TemplateFile $Template_Location -TemplateParameterFile $Template_Parameters

