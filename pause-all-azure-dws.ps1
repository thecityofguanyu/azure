<#   
.NOTES     
    Author: Chris A. Evans
    Email: thecityofguanyu@outlook.com

.SYNOPSIS
    Stops inactive Azure Synapse Workspace Dedicated Pools 
   
.DESCRIPTION
    A crude frankenstein amalgam of the following:
      * https://github.com/FonsecaSergio/ScriptCollection/blob/master/Powershell/Synapse%20-%20Pause%20all%20DWs%20-%20Automation%20Acount.ps1
      * https://github.com/sagu94271/seaa/blob/main/Devops/Powershell/autoshutsynapsesqlpool.ps1
 
.PARAMETER debug  
#> 

param (
    [bool]$debug = $false
)

Clear-Host


##########################################################################################################################################################
Import-Module Az.Accounts
Import-Module Az.Sql
Import-Module Az.Synapse
Import-Module SqlServer
##########################################################################################################################################################
#Parameters
##########################################################################################################################################################
$ErrorActionPreference = "Continue"

##########################################################################################################################################################
#VARIABLES
##########################################################################################################################################################
[int]$iErrorCount = 0
[System.Collections.ArrayList]$AzureSQLServers = @()
[System.Collections.ArrayList]$AzureSynapseWorkspaces = @()
[System.Collections.ArrayList]$SynapseSqlPools = @()

##########################################################################################################################################################
#CONNECT TO AZURE
##########################################################################################################################################################

# Ensures you do not inherit an AzContext in your runbook
Disable-AzContextAutosave -Scope Process | Out-Null

# Connect using a Managed Service Identity
try
{
    $AzureContext = (Connect-AzAccount -Identity).context
	Set-AzContext -SubscriptionName $AzureContext.Subscription -DefaultProfile $AzureContext
}
catch
{
    Write-Output "There is no system-assigned user identity. Aborting."; 
    exit
}

########################################################################################################


##########################################################################################################################################################
#Get SQL / Synapse RESOURCES
##########################################################################################################################################################
Write-Output ""
Write-Output "---------------------------------------------------------------------------------------------------"
Write-Output "Get SQL / Synapse RESOURCES"
Write-Output "---------------------------------------------------------------------------------------------------"

try {
    $AzureSynapseWorkspaces = @(Get-AzSynapseWorkspace | Where-Object { $_.ExtraProperties.WorkspaceType -eq "Normal" } -ErrorAction Stop)
}
catch {
    $iErrorCount += 1;
    Write-Error $_.Exception.Message
}

##########################################################################################################################################################
# Loop through all Synapse Workspaces
##########################################################################################################################################################
Write-Output ""
Write-Output "---------------------------------------------------------------------------------------------------"
Write-Output "Loop through all Synapse Workspaces"
Write-Output "---------------------------------------------------------------------------------------------------"

for ($i = 0; $i -lt $AzureSynapseWorkspaces.Count; $i++) {
    $AzureSynapseWorkspace = $AzureSynapseWorkspaces[$i]

    Write-Output " ***************************************************************************************"
    Write-Output " Checking Azure Synapse Workspace [$($AzureSynapseWorkspace.Name)] for Synapse SQL Pools"
    
    try {
        $SynapseSqlPools = @($AzureSynapseWorkspace | Get-AzSynapseSqlPool -ErrorAction Stop)
    }
    catch [Microsoft.Azure.Commands.Synapse.Models.Exceptions.SynapseException] {
        if ($_.Exception.InnerException.Message -eq "Operation returned an invalid status code 'Conflict'") {
            Write-Error "  -> Operation returned an invalid status code 'Conflict'"
            Write-Output "  -> Removed ($($AzureSynapseWorkspace.Name)) from AzureSynapseWorkspaces"            
            $AzureSynapseWorkspaces.Remove($AzureSynapseWorkspace);
        }
        else {
            $iErrorCount += 1;
            Write-Error $_.Exception.Message
        }
    }
    catch {
        $iErrorCount += 1;
        Write-Error $_.Exception.Message
    }
    
    foreach ($SynapseSqlPool in $SynapseSqlPools) {
        
        ##########################################################################################################################################################
        if ($SynapseSqlPool.Status -eq "Paused") {
            Write-Output "  -> Synapse SQL Pool [$($SynapseSqlPool.SqlPoolName)] found with status [Paused]"
        }
        ##########################################################################################################################################################
        elseif ($SynapseSqlPool.Status -eq "Online") {
            Write-Output "  -> Synapse SQL Pool [$($SynapseSqlPool.SqlPoolName)] found with status [Online]"
            Write-Output "  -> Checking if Synapse SQL Pool [$($SynapseSqlPool.SqlPoolName)] has been idle for the past 30 minutes"
            $kv = Get-AzKeyVault -VaultName "kv-gtp-lakehouse*"
            $connstring = Get-AzKeyVaultSecret -VaultName $kv.VaultName -Name "conn-synw" -AsPlainText

            $params = @{
            	'ConnectionString' = $connstring
            	'OutputSqlErrors'  = $true
            	'Query'            = 'if exists
            	( select * from sys.dm_pdw_exec_sessions where status in (''ACTIVE'',''IDLE'') and (session_id <> session_id()) 
            	and (app_name not in (''Internal'',''DWShellDb'')) and (status in (''IDLE'') and login_time > DATEADD(minute, -30, GETDATE()))
            	)
            		begin
            			select 1;
            		end
            	else
            		begin
            			select 0;
            		end'
            	}

            $transactions = Invoke-Sqlcmd  @params
            if ($transactions.Column1 -ne 0)
            {
            	Write-Output "  -> There are active transactions with 30 minutes. Not pausing"
            	continue
            }
            else
            {
            	Write-Output "  -> No activte transactions detected. Moving forward with pause operation."
            }

            # Pause Synapse SQL Pool
            $startTimePause = Get-Date
            Write-Output "  -> Pausing Synapse SQL Pool [$($SynapseSqlPool.SqlPoolName)]"
            
            if (!$debug) {
                $resultsynapseSqlPool = $SynapseSqlPool | Suspend-AzSynapseSqlPool    
            }
            
            # Show that the Synapse SQL Pool has been pause and how long it took
            $endTimePause = Get-Date
            $durationPause = NEW-TIMESPAN –Start $startTimePause –End $endTimePause

            if ($resultsynapseSqlPool.Status -eq "Paused") {
                Write-Output "  -> Synapse SQL Pool [$($resultsynapseSqlPool.SqlPoolName)] paused in $($durationPause.Hours) hours, $($durationPause.Minutes) minutes and $($durationPause.Seconds) seconds. Current status [$($resultsynapseSqlPool.Status)]"
            }
            else {
                if (!$debug) {
                    $iErrorCount += 1;
                    Write-Error "  -> (resultsynapseSqlPool.Status -ne ""Paused"") - Synapse SQL Pool [$($resultsynapseSqlPool.SqlPoolName)] paused in $($durationPause.Hours) hours, $($durationPause.Minutes) minutes and $($durationPause.Seconds) seconds. Current status [$($resultsynapseSqlPool.Status)]"
                }
                else {
                    Write-Host "This is a debug session - Nothing was done" -ForegroundColor Yellow
                }
            }           
        }
        ##########################################################################################################################################################
        else {
            $iErrorCount += 1;
            Write-Error "  -> (SynapseSqlPool.Status -eq ""Online"") Checking Synapse SQL Pool [$($SynapseSqlPool.SqlPoolName)] found with status [$($SynapseSqlPool.Status)]"
        }
        ##########################################################################################################################################################
    }    
}

##########################################################################################################################################################
if ($iErrorCount > 0) {
    Write-Error -Message "Pause DB script error count ($($iErrorCount)) check logs" `
        -Exception ([System.Exception]::new()) -ErrorAction Stop 
}
