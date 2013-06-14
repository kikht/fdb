#include "options.h"

static HvaultColumnInfo
get_column_type(Oid foreigntableid, AttrNumber attnum)
{
    List *colopts;
    ListCell *m;
    HvaultColumnInfo res;

    res.type = HvaultColumnDataset;
    res.name = NULL;
    colopts = GetForeignColumnOptions(foreigntableid, attnum);
    foreach(m, colopts)
    {
        DefElem *opt = (DefElem *) lfirst(m);
        if (strcmp(opt->defname, "type") == 0)
        {
            char *type = defGetString(opt);

            if (strcmp(type, "point") == 0) 
            {
                res.type = HvaultColumnPoint;
            }
            else if (strcmp(type, "footprint") == 0)
            {
                res.type = HvaultColumnFootprint;   
            }
            else if (strcmp(type, "catalog") == 0) 
            {
                res.type = HvaultColumnCatalog;
            }
            else if (strcmp(type, "index") == 0)
            {
                res.type = HvaultColumnIndex;
            }
            else if (strcmp(type, "line_index") == 0)
            {
                res.type = HvaultColumnLineIdx;
            }
            else if (strcmp(type, "sample_index") == 0)
            {
                res.type = HvaultColumnSampleIdx;
            }
            else if (strcmp(type, "dataset") == 0)
            {
                res.type = HvaultColumnDataset;
            }
            else
            {
                ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                                errmsg("Unknown column type %s", type)));
            }
        }
        else if (strcmp(opt->defname, "name") == 0)
        {
            res.name = defGetString(opt);
        }
    }

    return res;
}

typedef struct 
{
    HvaultColumnInfo *info;
    Index relid;
    Oid foreigntableid;
    AttrNumber natts;
} HvaultColumnTypeWalkerContext;

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
            if (cxt->info[attnum].type == HvaultColumnNull)
            {
                cxt->info[attnum] = get_column_type(cxt->foreigntableid, 
                                                     var->varattno);
            }
        }
    }
    return expression_tree_walker(node, get_column_types_walker, (void *) cxt);
}

HvaultColumnInfo *
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
    walker_cxt.info = palloc(sizeof(HvaultColumnInfo) * walker_cxt.natts);
    for (attnum = 0; attnum < walker_cxt.natts; attnum++)
    {
        walker_cxt.info[attnum].type = HvaultColumnNull;
        walker_cxt.info[attnum].name = NULL;
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

    return walker_cxt.info;
}

// List *
// hvaultGetAllColumns(Relation relation)
// {
//     List *result = NIL;
//     AttrNumber attnum, natts;
//     Oid foreigntableid = RelationGetRelid(relation);

//     natts = RelationGetNumberOfAttributes(relation);
//     for (attnum = 0; attnum < natts; ++attnum)
//     {
//         result = lappend_int(result, get_column_type(foreigntableid, attnum+1));
//     }

//     return result;
// }

DefElem * 
hvaultGetTableOption(Oid foreigntableid, char *option)
{
    ListCell *l;
    ForeignTable *foreigntable = GetForeignTable(foreigntableid);
    foreach(l, foreigntable->options)
    {
        DefElem *def = (DefElem *) lfirst(l);
        if (strcmp(def->defname, option) == 0)
        {
            return def;
        }
    }
    elog(DEBUG1, "Can't find table option %s", option);
    return NULL;
}

