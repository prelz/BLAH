//
//  File :     classad_binary_op_unwind.C
//
//
//  Author :   Francesco Prelz ($Author: fprelz $)
//  e-mail :   "francesco.prelz@mi.infn.it"
//
//  Revision history :
//   3-Mar-2006 Original release
//
//  Description:
//   Classad Unparser spaecialisation to extract the arguments
//   of binary operations of a given ExprTree.
//
//  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
//   All rights reserved.
//   See http://grid.infn.it/grid/license.html for license details.

#include <vector>
#include <string>
#include <sstream>

#include <strings.h> // strcasecmp

#include "classad_distribution.h"
#include "classad_binary_op_unwind.h"

#ifdef WANT_NAMESPACES
namespace classad {
#endif

// BinaryOpUnwind object implementation
BinaryOpUnwind::
BinaryOpUnwind( )
{
}


BinaryOpUnwind::
~BinaryOpUnwind( )
{
}

void BinaryOpUnwind::
UnparseAux(std::string &buffer,Operation::OpKind op, ExprTree *t1, ExprTree *t2,
	   ExprTree *t3)
{
  // We are interested just in a subset of binary operations

  if ( op == Operation::LESS_THAN_OP ||
       op == Operation::GREATER_THAN_OP ||
       op == Operation::LESS_OR_EQUAL_OP ||
       op == Operation::GREATER_OR_EQUAL_OP ||
       op == Operation::EQUAL_OP ||
       op == Operation::IS_OP )
   {
    // Check that we have one attribute reference and one literal.
    ExprTree *attr, *value;
    bool args_ok = false;
    bool value_at_right;
    bool numeric_value;
    std::string attribute_name;
    std::string attribute_value; 

    if (t1->GetKind() == ExprTree::LITERAL_NODE &&
        t2->GetKind() == ExprTree::ATTRREF_NODE)
     {
      args_ok = true;
      attr = t2;
      value = t1;
      value_at_right = false;
     }
    if (t2->GetKind() == ExprTree::LITERAL_NODE &&
        t1->GetKind() == ExprTree::ATTRREF_NODE)
     {
      args_ok = true;
      attr = t1;
      value = t2;
      value_at_right = true;
     }

    numeric_value = false;

    if (args_ok)
     {
      ExprTree *expr;
      bool    absolute;
      // We just care about the attribute name, not about its base ad.
      ((AttributeReference*)attr)->GetComponents(expr, attribute_name, absolute);

      Value v;
      EvalState state;
      //state.SetScopes( ad );

      value->Evaluate(state,v);
      int intres;
      std::string strres;

      if (v.IsIntegerValue( intres ))
       {
        Unparse( attribute_value, v );
        numeric_value = true;
       }
      else if (v.IsStringValue( strres ))
       {
        attribute_value = strres;
       }
      else
       {
        args_ok = false;
       }
     }

    /* Comparison on numeric value ?*/
    if ((!numeric_value) &&
         op != Operation::EQUAL_OP && op != Operation::IS_OP)
      args_ok = false;

    if (args_ok)
     {
      if ( ( value_at_right  && ( op == Operation::LESS_THAN_OP || op == Operation::LESS_OR_EQUAL_OP ) ) ||
           ( !value_at_right && ( op == Operation::GREATER_THAN_OP || op == Operation::GREATER_OR_EQUAL_OP ) ) )
       {
        std::string result_line = attribute_name;
        result_line.append("_Max='");
        result_line.append(attribute_value);
        result_line.append("'");
        m_unwind_output.push_back(result_line);
       }

      if ( ( !value_at_right  && ( op == Operation::LESS_THAN_OP || op == Operation::LESS_OR_EQUAL_OP ) ) ||
           ( value_at_right && ( op == Operation::GREATER_THAN_OP || op == Operation::GREATER_OR_EQUAL_OP ) ) )
       {
        std::string result_line = attribute_name;
        result_line.append("_Min='");
        result_line.append(attribute_value);
        result_line.append("'");
        m_unwind_output.push_back(result_line);
       }

      if ( op == Operation::EQUAL_OP ||
           op == Operation::IS_OP )
       {
        std::string result_line = attribute_name;
        result_line.append("='");
        result_line.append(attribute_value);
        result_line.append("'");
        m_unwind_output.push_back(result_line);
       }

     }
    
   }

  // Fall back to base class to complete unparsing.
  ClassAdUnParser::UnparseAux( buffer, op, t1, t2, t3 );
  return;
}

void BinaryOpUnwind::
UnparseAux( std::string &buffer, std::string &fnName, std::vector<ExprTree*>& args )
{
  if (strcasecmp(fnName.c_str(),"member") == 0)
   {
    if ((args[0])->GetKind() == ExprTree::LITERAL_NODE &&
        (args[1])->GetKind() == ExprTree::ATTRREF_NODE)
     {
      std::string attribute_name;
      std::string attribute_value;

      ExprTree *expr;
      bool    absolute;
      // We just care about the attribute name, not about its base ad.
      ((AttributeReference*)args[1])->GetComponents(expr, attribute_name, absolute);

      Value v;
      EvalState state;

      args[0]->Evaluate(state,v);
      Unparse( attribute_value, v );

      std::stringstream result_line;
      result_line << attribute_name << "[" << m_member_list_counter_[attribute_name]++ << "]=" << attribute_value;
      m_unwind_output.push_back(result_line.str());
     }
   }

  // Fall back to base class to complete unparsing.
  ClassAdUnParser::UnparseAux( buffer, fnName, args );
  return;
}

#ifdef WANT_NAMESPACES
} // end of classad namespace
#endif
