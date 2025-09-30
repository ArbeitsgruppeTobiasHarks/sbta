use std::{fs::File, io::Read};

use crate::lp::LinearProgram;
use gurobi::{
    Env, LinExpr, Model,
    attr::{
        CharAttr, DoubleAttr,
        IntAttr::{self},
    },
    param::IntParam,
};

use super::{Flt, LPConstrType, LPStatus};

impl Into<LPStatus> for gurobi::Status {
    fn into(self) -> LPStatus {
        match self {
            gurobi::Status::Optimal => LPStatus::OPTIMAL,
            gurobi::Status::Infeasible => LPStatus::INFEASIBLE,
            gurobi::Status::Unbounded => LPStatus::UNBOUNDED,
            _ => LPStatus::UNKNOWN,
        }
    }
}

impl Into<crate::lp::LPConstrType> for gurobi::ConstrSense {
    fn into(self) -> LPConstrType {
        match self {
            gurobi::ConstrSense::Less => LPConstrType::LEQ,
            gurobi::ConstrSense::Equal => LPConstrType::EQ,
            gurobi::ConstrSense::Greater => LPConstrType::GEQ,
        }
    }
}

impl Into<gurobi::ConstrSense> for crate::lp::LPConstrType {
    fn into(self) -> gurobi::ConstrSense {
        match self {
            LPConstrType::EQ => gurobi::ConstrSense::Equal,
            LPConstrType::LEQ => gurobi::ConstrSense::Less,
            LPConstrType::GEQ => gurobi::ConstrSense::Greater,
        }
    }
}

thread_local! {
    static ENV: gurobi::Env = {
        let mut env = Env::new("gurobi.log").unwrap();
        env.set(IntParam::OutputFlag, 0).unwrap();
        env
    };
}

impl LinearProgram for Model {
    type Constr = gurobi::Constr;

    type Var = gurobi::Var;

    fn create() -> Self {
        ENV.with(|env| Model::new("", env).unwrap())
    }

    fn reset(&mut self) {
        todo!()
    }

    fn set_minimize(&mut self) {
        self.set(IntAttr::ModelSense, gurobi::ModelSense::Minimize.into())
            .unwrap();
    }

    fn get_objval(&self) -> super::Flt {
        self.get(DoubleAttr::ObjVal).unwrap()
    }

    fn optimize(&mut self) {
        self.optimize().unwrap();
    }

    fn get_status(&self) -> super::LPStatus {
        self.status().unwrap().into()
    }

    fn add_constr_with_rhs(
        &mut self,
        constr_type: super::LPConstrType,
        rhs: super::Flt,
    ) -> Self::Constr {
        self.add_constr("", LinExpr::new(), constr_type.into(), rhs)
            .unwrap()
    }

    fn set_constr_bound(
        &mut self,
        constr: &Self::Constr,
        constr_type: LPConstrType,
        rhs: super::Flt,
    ) {
        constr
            .set(
                self,
                CharAttr::Sense,
                Into::<gurobi::ConstrSense>::into(constr_type).into(),
            )
            .unwrap();
        constr.set(self, DoubleAttr::RHS, rhs).unwrap();
    }

    fn get_coeff(&self, constr: &Self::Constr, var: &Self::Var) -> super::Flt {
        self.get_coeff(var, constr).unwrap()
    }

    fn set_coeff(&mut self, constr: &Self::Constr, var: &Self::Var, coeff: super::Flt) {
        self.set_coeff(var, constr, coeff).unwrap();
    }

    fn get_constr_dual_val(&self, constr: &Self::Constr) -> super::Flt {
        constr.get(self, DoubleAttr::Pi).unwrap()
    }

    fn add_var(
        &mut self,
        lower: super::Flt,
        upper: super::Flt,
        obj: super::Flt,
        colconstrs: &[Self::Constr],
        colvals: &[Flt],
    ) -> Self::Var {
        self.add_var(
            "",
            gurobi::VarType::Continuous,
            obj,
            lower,
            upper,
            colconstrs,
            colvals,
        )
        .unwrap()
    }

    fn remove_var(&mut self, var: Self::Var) {
        self.remove(var)
    }

    fn get_var_val(&self, var: &Self::Var) -> super::Flt {
        var.get(self, DoubleAttr::X).unwrap()
    }

    fn get_var_status(&self, var: &Self::Var) -> super::VarStatus {
        match var.get(self, IntAttr::VBasis).unwrap() {
            0 => super::VarStatus::Basic,
            -1 => super::VarStatus::NonbasicLower,
            -2 => super::VarStatus::NonbasicUpper,
            -3 => super::VarStatus::Superbasic,
            it => panic!("Unknown variable status {:}.", it),
        }
    }

    fn set_var_obj(&mut self, var: &Self::Var, obj: super::Flt) {
        var.set(self, DoubleAttr::Obj, obj).unwrap();
    }

    fn set_var_bound(&mut self, var: &Self::Var, lower: super::Flt, upper: super::Flt) {
        var.set(self, DoubleAttr::LB, lower).unwrap();
        var.set(self, DoubleAttr::UB, upper).unwrap();
    }

    fn get_var_reduced_cost(&self, var: &Self::Var) -> super::Flt {
        var.get(self, DoubleAttr::RC).unwrap()
    }

    fn inf() -> super::Flt {
        gurobi::INFINITY
    }

    fn print(&self) {
        self.write("model.lp").unwrap();
        let mut file = File::open("model.lp").unwrap();
        let mut buf = String::new();
        file.read_to_string(&mut buf).unwrap();
        println!("{}", buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lp::{LinearProgram, VarStatus};

    fn test<LP: LinearProgram>() {
        let mut lp = LP::create();
        lp.set_minimize();
        let x = lp.add_var(0.0, 0.5, -1.0, &[], &[]);
        let y = lp.add_var(0.0, 0.5, -1.0, &[], &[]);
        let constr = lp.add_constr_with_rhs(LPConstrType::GEQ, 1.0);
        lp.set_coeff(&constr, &x, 1.0);
        lp.set_coeff(&constr, &y, 1.0);
        lp.set_constr_bound(&constr, LPConstrType::LEQ, 1.0);
        lp.optimize();
        assert_eq!(lp.get_status(), LPStatus::OPTIMAL);
        assert_eq!(lp.get_objval(), -1.0);
        assert_eq!(lp.get_var_val(&x), 0.5);
        assert_eq!(lp.get_var_val(&y), 0.5);
        assert_eq!(lp.get_var_status(&x), VarStatus::NonbasicUpper);
        assert_eq!(lp.get_var_status(&y), VarStatus::NonbasicUpper);
        lp.remove_var(x);
        lp.remove_var(y);
    }

    #[test]
    fn test_gurobi() {
        test::<Model>();
    }
}
