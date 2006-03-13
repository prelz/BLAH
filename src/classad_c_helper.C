//
//  File :     classad_c_helper.C
//
//
//  Author :   Francesco Prelz ($Author: gfiorent $)
//  e-mail :   "francesco.prelz@mi.infn.it"
//
//  Revision history :
//   5-Apr-2004 Original release
//  16-Apr-2004 Added string list parse and unparse.
//   7-May-2004 Added retrieval of string argument into dynamic string.
//  19-Aug-2004 Added boolean attribute.
//
//  Description:
//   c-callable layer for handling Classad parse and unparse via the 'new'
//   ClassAd library.
//   
//  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN). 
//  All rights reserved.
//  See http://grid.infn.it/grid/license.html for license details. 
//

#include <string.h>
#include "classad_distribution.h"
#include "classad_binary_op_unwind.h"

#ifdef WANT_NAMESPACES
using namespace classad;
#endif

extern "C"
 {

#include <string.h>
#include "classad_c_helper.h"

  classad_context 
  classad_parse (char *s_in)
   {
    ClassAd *ad = NULL;
    ClassAdParser parser;
    ad = parser.ParseClassAd(s_in);
  
    // ad == NULL on error.
    return ((classad_context)ad);
   }

  char *
  classad_unparse (classad_context cad)
   {
    if (cad == NULL) return NULL;

    std::string res_s;
    char *res_c;

    ClassAd *ad = (ClassAd *)cad;
    ClassAdUnParser unparser;
    unparser.Unparse(res_s, ad);
  
    if (res_s.size() > 0)
     {
      res_c = strdup(res_s.c_str());
     }
    // res_c == NULL on error.
    return (res_c);
   }

  int
  classad_get_string_attribute (classad_context cad, const char *attribute_name, 
                                char *result, int l_result)
   {
    if (cad == NULL) return C_CLASSAD_INVALID_CONTEXT;

    ClassAd *ad = (ClassAd *)cad;

    Value v;
    ad->EvaluateAttr(attribute_name, v);

    if (v.IsStringValue( result, l_result ))
     {
      return C_CLASSAD_NO_ERROR;
     }
    
    return C_CLASSAD_VALUE_NOT_FOUND;
   }

  int
  classad_get_dstring_attribute (classad_context cad, const char *attribute_name, 
                                 char **result)
   {
    if (cad == NULL) return C_CLASSAD_INVALID_CONTEXT;

    ClassAd *ad = (ClassAd *)cad;

    Value v;
    ad->EvaluateAttr(attribute_name, v);

    std::string res_str;

    if (v.IsStringValue( res_str ))
     {
      (*result) = strdup(res_str.c_str());
      if ((*result) == NULL) return C_CLASSAD_OUT_OF_MEMORY;
      return C_CLASSAD_NO_ERROR;
     }
    
    return C_CLASSAD_VALUE_NOT_FOUND;
   }

  void
  classad_free_string_list(char **strl)
   {
    if (strl != NULL)
     {
      char **str_val;
      for(str_val=strl; (*str_val) != NULL; str_val++) 
       {
        if ((*str_val)!=NULL) free(*str_val);
       }
      free(strl);
     }
   }

  int
  classad_get_string_list_attribute (classad_context cad, 
                                     const char *attribute_name, 
                                     char ***result)
   {
    if (cad == NULL) return C_CLASSAD_INVALID_CONTEXT;

    int n_results = 0;
    (*result) = (char **)malloc(sizeof(char **));
    if ((*result) == NULL) return C_CLASSAD_OUT_OF_MEMORY;

    (*result)[0] = NULL;

    ClassAd *ad = (ClassAd *)cad;

    Value vl;
    ad->EvaluateAttr(attribute_name, vl);

    const ExprList *et_result;
    if (vl.IsListValue(et_result)) 
     {
      std::vector<ExprTree*> ads;
      et_result->GetComponents(ads);
      // Get string values.
      for(std::vector<ExprTree*>::const_iterator it = ads.begin();
          it != ads.end(); it++) 
       {
        if ((*it)->GetKind() == ExprTree::LITERAL_NODE) 
         {
          Value v;
          EvalState       state;
          state.SetScopes( ad );

          (*it)->Evaluate(state,v);

          std::string res_str;
          if (v.IsStringValue( res_str ))
           {
            // add string value to result, which is a NULL-terminated
            // string array.
            n_results++;
            (*result) = (char **)realloc(*result, (n_results+1)*sizeof(char *));
            if ((*result) == NULL) return C_CLASSAD_OUT_OF_MEMORY;
            (*result)[n_results-1] = strdup(res_str.c_str());
            (*result)[n_results] = NULL;
           }
         }
       }
      return C_CLASSAD_NO_ERROR;
     }
    
    // The result list needs to be freed on success only.
    classad_free_string_list(*result);
    (*result) = NULL;
    return C_CLASSAD_VALUE_NOT_FOUND;
   }

  int
  classad_get_int_attribute (classad_context cad, const char *attribute_name, 
                             int *result)
   {
    if (cad == NULL) return C_CLASSAD_INVALID_CONTEXT;

    ClassAd *ad = (ClassAd *)cad;

    Value v;
    ad->EvaluateAttr(attribute_name, v);

    if (v.IsIntegerValue( *result ))
     {
      return C_CLASSAD_NO_ERROR;
     }
    
    return C_CLASSAD_VALUE_NOT_FOUND;
   }

  int
  classad_get_bool_attribute (classad_context cad, const char *attribute_name, 
                              int *result)
   {
    if (cad == NULL) return C_CLASSAD_INVALID_CONTEXT;

    ClassAd *ad = (ClassAd *)cad;

    Value v;
    ad->EvaluateAttr(attribute_name, v);

    bool tmp_res;

    if (v.IsBooleanValue( tmp_res ))
     {
      if (tmp_res) *result = 1;
      else         *result = 0;
      return C_CLASSAD_NO_ERROR;
     }
    
    return C_CLASSAD_VALUE_NOT_FOUND;
   }

  int
  classad_put_string_attribute (classad_context *cad, const char *name, 
                                const char *value)
   {
    ClassAd *ad;
    if ((*cad) == NULL) 
     {
      ad = new ClassAd;
      (*cad) = (classad_context) ad;
     }
    else ad = (ClassAd *)(*cad);

    std::string str_val(value);

    if (ad->InsertAttr (name, str_val)) return C_CLASSAD_NO_ERROR; 
    else                                return C_CLASSAD_INSERT_FAILED;
   }

  int
  classad_put_string_list_attribute (classad_context *cad, 
                                     const char *name, 
                                     char **value)
   {
    if (value == NULL) return C_CLASSAD_INVALID_VALUE;

    ClassAd *ad;
    if ((*cad) == NULL) 
     {
      ad = new ClassAd;
      (*cad) = (classad_context) ad;
     }
    else ad = (ClassAd *)(*cad);

    char **str_val;

    std::vector<ExprTree*> et_ads;

    // Traverse NULL-terminated string array.
    for(str_val=value; (*str_val) != NULL; str_val++) 
     {
      Value v;
      v.SetStringValue(*str_val);
      et_ads.push_back(Literal::MakeLiteral(v));
     }

    ExprList *et_value;
    et_value = ExprList::MakeExprList(et_ads);

    if (ad->Insert (name, et_value)) return C_CLASSAD_NO_ERROR; 
    else                             return C_CLASSAD_INSERT_FAILED;
   }

  int
  classad_put_int_attribute (classad_context *cad, const char *name, 
                             int value)
   {
    ClassAd *ad;
    if ((*cad) == NULL) 
     {
      ad = new ClassAd;
      (*cad) = (classad_context) ad;
     }
    else ad = (ClassAd *)(*cad);

    if (ad->InsertAttr (name, value)) return C_CLASSAD_NO_ERROR; 
    else                              return C_CLASSAD_INSERT_FAILED;
   }

  int
  classad_put_bool_attribute (classad_context *cad, const char *name, 
                              int value)
   {
    ClassAd *ad;
    if ((*cad) == NULL) 
     {
      ad = new ClassAd;
      (*cad) = (classad_context) ad;
     }
    else ad = (ClassAd *)(*cad);

    bool tmp_value;

    if (value != 0) tmp_value = true;
    else            tmp_value = false;

    if (ad->InsertAttr (name, tmp_value)) return C_CLASSAD_NO_ERROR; 
    else                                  return C_CLASSAD_INSERT_FAILED;

   }

  void
  classad_free (classad_context cad)
   {
    ClassAd *ad;
    if (cad != NULL)
     {
      ad = (ClassAd *)cad;
      delete ad;
     }
   }

  void
  classad_dump (classad_context cad)
   {
    ClassAd *ad;
    if (cad != NULL)
     {
      ad = (ClassAd *)cad;
      ad->Puke();
     }
   }

  int
  unwind_attributes(classad_context cad, char *attribute_name, char ***results) 
   {
    if (cad == NULL) return C_CLASSAD_INVALID_CONTEXT;

    ClassAd *ad = (ClassAd *)cad;

    ExprTree *et;
    bool need_to_delete_et = false;

    et = ad->Lookup(attribute_name);
    if (et == NULL)
     {
      return C_CLASSAD_VALUE_NOT_FOUND;
     }

    if (et->GetKind() == ExprTree::LITERAL_NODE)
     {
      // The attribute was probably stringified. Try to parse it.
      Value v;
      EvalState state;
      state.SetScopes( ad );

      et->Evaluate(state,v);
      std::string strres;

      if (v.IsStringValue( strres ))
       {
        ClassAdParser parser;
        et=NULL;
        parser.ParseExpression(strres,et);
        need_to_delete_et = true;
       }
     }

    BinaryOpUnwind res_unp; 
    std::string result;
    res_unp.Unparse(result, et);
    int n_results = 0;
    (*results) = (char **)malloc(sizeof(char **));
    if ((*results) == NULL) return C_CLASSAD_OUT_OF_MEMORY;

    std::vector<std::string>::const_iterator it;
    for (it = res_unp.m_unwind_output.begin(); 
         it != res_unp.m_unwind_output.end(); it++)
     {
      n_results++;
      (*results) = (char **)realloc(*results, (n_results+1)*sizeof(char *));
      if ((*results) == NULL) return C_CLASSAD_OUT_OF_MEMORY;
      (*results)[n_results-1] = strdup(it->c_str());
      (*results)[n_results] = NULL;
     }

    if (need_to_delete_et) delete et;
    return C_CLASSAD_NO_ERROR;
   }

 } // end of extern "C"
