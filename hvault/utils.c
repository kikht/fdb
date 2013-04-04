#include "hvault.h"
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <foreign/foreign.h>
#include <nodes/nodeFuncs.h>
#include <utils/rel.h>

typedef struct 
{
    HvaultColumnType *types;
    Index relid;
    Oid foreigntableid;
    AttrNumber natts;
} HvaultColumnTypeWalkerContext;

static HvaultColumnType
get_column_type(Oid foreigntableid, AttrNumber attnum)
{
    List *colopts;
    ListCell *m;
    HvaultColumnType res;

    res = HvaultColumnFloatVal;
    colopts = GetForeignColumnOptions(foreigntableid, attnum);
    foreach(m, colopts)
    {
        DefElem *opt = (DefElem *) lfirst(m);
        if (strcmp(opt->defname, "type") == 0)
        {
            char *type = defGetString(opt);

            if (strcmp(type, "float") == 0)
            {
                res = HvaultColumnFloatVal;
            }
            else if (strcmp(type, "byte") == 0) 
            {
                res = HvaultColumnInt8Val;
            }
            else if (strcmp(type, "int2") == 0) 
            {
                res = HvaultColumnInt16Val;
            }
            else if (strcmp(type, "int4") == 0) 
            {
                res = HvaultColumnInt32Val;
            }
            else if (strcmp(type, "int8") == 0) 
            {
                res = HvaultColumnInt32Val;
            }
            else if (strcmp(type, "point") == 0) 
            {
                res = HvaultColumnPoint;
            }
            else if (strcmp(type, "footprint") == 0)
            {
                res = HvaultColumnFootprint;   
            }
            else if (strcmp(type, "file_index") == 0)
            {
                res = HvaultColumnFileIdx;
            }
            else if (strcmp(type, "line_index") == 0)
            {
                res = HvaultColumnLineIdx;
            }
            else if (strcmp(type, "sample_index") == 0)
            {
                res = HvaultColumnSampleIdx;
            }
            else if (strcmp(type, "time") == 0)
            {
                res = HvaultColumnTime;
            }
            else
            {
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Unknown column type %s", 
                                       type)));
            }
            elog(DEBUG1, "col: %d strtype: %s type: %d", attnum, type, res);
        }
    }

    return res;
}

static bool 
get_column_types_walker(Node *node, HvaultColumnTypeWalkerContext *cxt)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varno == cxt->relid)
        {
            AttrNumber attnum;
            Assert(var->varattno <= cxt->natts);
            attnum = var->varattno-1;
            if (cxt->types[attnum] == HvaultColumnNull)
            {
                cxt->types[attnum] = get_column_type(cxt->foreigntableid, 
                                                     var->varattno);
            }
        }
    }
    return expression_tree_walker(node, get_column_types_walker, (void *) cxt);
}

HvaultColumnType *
hvaultGetUsedColumns(PlannerInfo *root, 
                     RelOptInfo *baserel, 
                     Oid foreigntableid,
                     AttrNumber natts)
{
    ListCell *l, *m;
    AttrNumber attnum;
    HvaultColumnTypeWalkerContext walker_cxt;

    walker_cxt.natts = natts;
    walker_cxt.relid = baserel->relid;
    walker_cxt.foreigntableid = foreigntableid;
    walker_cxt.types = palloc(sizeof(HvaultColumnType) * walker_cxt.natts);
    for (attnum = 0; attnum < walker_cxt.natts; attnum++)
    {
        walker_cxt.types[attnum] = HvaultColumnNull;
    }

    get_column_types_walker((Node *) baserel->reltargetlist, &walker_cxt);
    foreach(l, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        get_column_types_walker((Node *) rinfo->clause, &walker_cxt);
    }

    foreach(l, baserel->joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);
        get_column_types_walker((Node *) rinfo->clause, &walker_cxt);
    }

    foreach(l, root->eq_classes)
    {
        EquivalenceClass *ec = (EquivalenceClass *) lfirst(l);
        if (!bms_is_member(baserel->relid, ec->ec_relids))
        {
            continue;
        }
        foreach(m, ec->ec_members)
        {
            EquivalenceMember *em = (EquivalenceMember *) lfirst(m);
            get_column_types_walker((Node *) em->em_expr, &walker_cxt);
        }
    }

    // for (attnum = 0; attnum < walker_cxt.natts; attnum++)
    // {
    //     res = lappend_int(res, walker_cxt.types[attnum]);
    // }
    // pfree(walker_cxt.types);
    return walker_cxt.types;
}

List *
hvaultGetAllColumns(Relation relation)
{
    List *result = NIL;
    AttrNumber attnum, natts;
    Oid foreigntableid = RelationGetRelid(relation);

    natts = RelationGetNumberOfAttributes(relation);
    for (attnum = 0; attnum < natts; ++attnum)
    {
        result = lappend_int(result, get_column_type(foreigntableid, attnum+1));
    }

    return result;
}

char * 
hvaultGetTableOption(Oid foreigntableid, char *option)
{
    ListCell *l;
    ForeignTable *foreigntable = GetForeignTable(foreigntableid);
    foreach(l, foreigntable->options)
    {
        DefElem *def = (DefElem *) lfirst(l);
        if (strcmp(def->defname, option) == 0)
        {
            return defGetString(def);
        }
    }
    elog(DEBUG1, "Can't find table option %s", option);
    return NULL;
}

double 
hvaultGetNumFiles(char *catalog)
{
    StringInfo query_str;
    Datum val;
    int64_t num_files;
    bool isnull;

    if (!catalog)
    {
        /* Single file mode */
        return 1;
    }

    if (SPI_connect() != SPI_OK_CONNECT)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                     errmsg("Can't connect to SPI")));
        return 0; /* Will never reach this */
    }

    query_str = makeStringInfo();
    /* Add constraints */
    appendStringInfo(query_str, "SELECT COUNT(*) FROM %s", catalog);
    if (SPI_execute(query_str->data, true, 1) != SPI_OK_SELECT || 
        SPI_processed != 1)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               catalog)));
        return 0; /* Will never reach this */      
    }
    pfree(query_str->data);
    pfree(query_str);
    if (SPI_tuptable->tupdesc->natts != 1 ||
        SPI_tuptable->tupdesc->attrs[0]->atttypid != INT8OID)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               catalog)));
        return 0; /* Will never reach this */         
    }
    val = heap_getattr(SPI_tuptable->vals[0], 1, 
                       SPI_tuptable->tupdesc, &isnull);
    if (isnull)
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't get number of rows in catalog %s", 
                               catalog)));
        return 0; /* Will never reach this */            
    }
    num_files = DatumGetInt64(val);
    if (SPI_finish() != SPI_OK_FINISH)    
    {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("Can't finish access to SPI")));
        return 0; /* Will never reach this */   
    }
    return num_files;
}
