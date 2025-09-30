pub mod lp;

use std::collections::HashMap;

use lp::Flt;

use crate::lp::LinearProgram;

const EPS: Flt = 1e-6;

#[derive(Debug, Clone)]
pub struct BundleRessourceUsage {
    pub ressource_id: usize,
    pub usage_per_unit: Flt,
}

pub struct Bundle {
    pub ressource_usages: Vec<BundleRessourceUsage>,
    pub original_cost: Flt,
}

pub struct MCRA<LP: LinearProgram> {
    pub lp: LP,
    ressources: HashMap<usize, LP::Constr>,
    bundles: HashMap<usize, (Bundle, LP::Var)>,
    commodities: HashMap<usize, LP::Constr>,
}

impl<LP: LinearProgram> MCRA<LP> {
    pub fn new() -> Self {
        let mut lp = LP::create();
        lp.set_minimize();
        MCRA {
            lp,
            ressources: HashMap::new(),
            bundles: HashMap::new(),
            commodities: HashMap::new(),
        }
    }

    pub fn add_commodity(&mut self, id: usize, demand: Flt) {
        assert!(
            !self.commodities.contains_key(&id),
            "Commodity with id {} already exists.",
            id
        );
        let constr = self.lp.add_constr_with_rhs(lp::LPConstrType::EQ, demand);
        self.commodities.insert(id, constr);
    }

    pub fn add_ressource(&mut self, id: usize, capacity: Flt) {
        assert!(
            !self.ressources.contains_key(&id),
            "Ressource with id {} already exists.",
            id
        );
        let constr = self.lp.add_constr_with_rhs(lp::LPConstrType::LEQ, capacity);
        self.ressources.insert(id, constr);
    }

    pub fn get_bundle_shadow_price(
        &self,
        bundle_id: usize,
        shadow_prices: &HashMap<usize, Flt>,
    ) -> Flt {
        let Some((bundle, _)) = self.bundles.get(&bundle_id) else {
            panic!("Bundle with id {} not found.", bundle_id);
        };
        bundle
            .ressource_usages
            .iter()
            .map(|usage| shadow_prices.get(&usage.ressource_id).unwrap() * usage.usage_per_unit)
            .sum()
    }

    pub fn add_bundle(
        &mut self,
        id: usize,
        commodity_id: usize,
        original_cost: Flt,
        ressource_usages: Vec<BundleRessourceUsage>,
    ) {
        assert!(
            !self.bundles.contains_key(&id),
            "Bundle with id {} already exists.",
            id
        );

        // The objective coefficient will be set before optimization.

        let mut colconstrs = Vec::<LP::Constr>::new();
        let mut colvals = Vec::<Flt>::new();

        let Some(commodity_constr) = self.commodities.get(&commodity_id) else {
            panic!(
                "Commodity with id {} referenced in bundle {} not found.",
                commodity_id, id
            );
        };
        colconstrs.push(commodity_constr.clone());
        colvals.push(1.0);

        for usage in &ressource_usages {
            let Some(ressource_constr) = self.ressources.get(&usage.ressource_id) else {
                panic!(
                    "Ressource with id {} referenced in bundle {} not found.",
                    usage.ressource_id, id
                );
            };
            colconstrs.push(ressource_constr.clone());
            colvals.push(usage.usage_per_unit);
        }
        let var = self
            .lp
            .add_var(0.0, LP::inf(), original_cost, &colconstrs, &colvals);

        self.bundles.insert(
            id,
            (
                Bundle {
                    ressource_usages,
                    original_cost,
                },
                var,
            ),
        );
    }

    pub fn remove_vars(&mut self) {
        let mut nonbasic_vars: Vec<_> = self.bundles.iter().filter_map(|(bundle_id, (_, var))| {
            if self.lp.get_var_status(var) == lp::VarStatus::NonbasicLower {
                Some((bundle_id, var))
            } else {
                None
            }
        })
        .map(|(bundle_id, var)| (*bundle_id, self.lp.get_var_reduced_cost(var)))
        .collect();

        // We remove the variables with the highest reduced cost.
        nonbasic_vars.sort_by(|a, b| b.1.total_cmp(&a.1));
        
        let percentage_cols_remaining = 0.5;
        let num_cols_to_remove = (nonbasic_vars.len() as f64 * percentage_cols_remaining).floor() as usize;
        println!("Removing {} variables with highest reduced cost.", num_cols_to_remove);
        for (bundle_id, rc) in nonbasic_vars.iter().take(num_cols_to_remove) {
            assert!(*rc >= 0.0);
            let (_, var) = self.bundles.remove(bundle_id).unwrap();
            self.lp.remove_var(var);
        }
    }

    pub fn solve<'a>(
        &mut self,
        bundle_solver: &'a mut impl MinCostBundleSolver,
    ) -> HashMap<usize, Flt> {
        loop {
            println!("Solving LP with {} variables and {} constraints.", self.bundles.len(), self.ressources.len() + self.commodities.len());
            self.lp.optimize();
            println!("LP solved.");

            assert_eq!(self.lp.get_status(), lp::LPStatus::OPTIMAL);

            // self.remove_vars();

            let shadow_prices = self
                .ressources
                .iter()
                .map(|(id, constr)| (*id, -self.lp.get_constr_dual_val(constr)))
                .collect();

            let mut jobs = Vec::<MinimalCostBundleJob>::new();

            for (commodity_id, commodity_constr) in &self.commodities {
                let cost_upper_bound = self.lp.get_constr_dual_val(commodity_constr);
                if cost_upper_bound > EPS {
                    jobs.push(MinimalCostBundleJob {
                        commodity_id: *commodity_id,
                        cost_upper_bound,
                    });
                }
                // There are no bundles with negative cost.
            }

            let bundles = bundle_solver.compute_batch(&shadow_prices, &jobs);
            if bundles.is_empty() {
                break;
            }
            let mut num_bundles_added = 0;

            for (job, bundle_id, bundle) in bundles {
                if !self.bundles.contains_key(&bundle_id) {
                    self.add_bundle(
                        bundle_id,
                        job.commodity_id,
                        bundle.original_cost,
                        bundle.ressource_usages,
                    );
                    num_bundles_added += 1;
                }
            }
            println!("Added {}/{} variables.", num_bundles_added, jobs.len());
        }

        let result = self
            .bundles
            .iter()
            .map(|(id, (_, var))| (*id, self.lp.get_var_val(var)))
            .filter(|(_, val)| *val > 0.0)
            .collect::<HashMap<usize, Flt>>();
        result
    }
}

pub struct MinimalCostBundleJob {
    pub commodity_id: usize,
    pub cost_upper_bound: Flt,
}

pub trait MinCostBundleSolver {
    fn compute_batch<'a>(
        &mut self,
        shadow_praces: &HashMap<usize, Flt>,
        jobs: &'a [MinimalCostBundleJob],
    ) -> Vec<(&'a MinimalCostBundleJob, usize, Bundle)>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        Bundle, BundleRessourceUsage, EPS, MCRA, MinCostBundleSolver, MinimalCostBundleJob, lp::Flt,
    };

    #[test]
    fn test_computes_minimal_capacitated_bundles() {
        struct Instance<'a> {
            bundles: &'a Vec<Vec<usize>>,
            // (capacity, cost)
            ressources: &'a Vec<(Flt, Flt)>,
        }

        let bundles = vec![vec![0, 1, 2], vec![1, 2, 3], vec![2, 3, 4], vec![3, 4, 5]];
        let ressources = vec![
            (0.0, 0.0),
            (1.0, 1.0),
            (2.0, 2.0),
            (3.0, 3.0),
            (4.0, 4.0),
            (5.0, 5.0),
        ];

        let mut instance = Instance {
            bundles: &bundles,
            ressources: &ressources,
        };

        let mut mcra = MCRA::<gurobi::Model>::new();

        for (ressource_id, (capacity, cost)) in ressources.iter().enumerate() {
            mcra.add_ressource(ressource_id, *capacity);
        }
        mcra.add_commodity(0, 3.0);
        mcra.add_bundle(
            bundles.len() - 1,
            0,
            instance.cost_of_bundle(bundles.len() - 1),
            instance.bundle_ressources(bundles.len() - 1),
        );
        impl<'a> Instance<'a> {
            fn cost_of_bundle(&self, bundle_id: usize) -> Flt {
                self.bundles[bundle_id]
                    .iter()
                    .map(|&ressource_id| self.ressources[ressource_id].1)
                    .sum()
            }
            fn bundle_ressources(&self, bundle_id: usize) -> Vec<BundleRessourceUsage> {
                self.bundles[bundle_id]
                    .iter()
                    .map(|&id| BundleRessourceUsage {
                        ressource_id: id,
                        usage_per_unit: 1.0,
                    })
                    .collect()
            }
        }
        impl<'a> MinCostBundleSolver for Instance<'a> {
            fn compute_batch<'b>(
                &mut self,
                shadow_prices: &HashMap<usize, Flt>,
                jobs: &'b [MinimalCostBundleJob],
            ) -> Vec<(&'b MinimalCostBundleJob, usize, Bundle)> {
                assert!(jobs.len() <= 1);
                let mut result: Vec<(&'b MinimalCostBundleJob, usize, Bundle)> =
                    Vec::<(&MinimalCostBundleJob, usize, Bundle)>::new();
                for job in jobs {
                    let costs: Vec<Flt> = self
                        .bundles
                        .iter()
                        .enumerate()
                        .map(|(id, bundle)| {
                            let new_cost = bundle
                                .iter()
                                .map(|&ressource_id| {
                                    self.ressources[ressource_id].1
                                        + shadow_prices.get(&ressource_id).unwrap()
                                })
                                .sum();
                            new_cost
                        })
                        .collect();
                    let (bundle_id, cost) = costs
                        .iter()
                        .enumerate()
                        .min_by(|a, b| a.1.total_cmp(b.1))
                        .unwrap();
                    if *cost < job.cost_upper_bound - EPS {
                        result.push((
                            job,
                            bundle_id,
                            Bundle {
                                ressource_usages: self.bundle_ressources(bundle_id),
                                original_cost: self.cost_of_bundle(bundle_id),
                            },
                        ));
                    }
                }
                result
            }
        }

        let result = mcra.solve(&mut instance);
        assert_eq!(result, HashMap::from([(1, 1.0), (2, 1.0), (3, 1.0)]));
    }
}
